/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.deployment;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import io.strimzi.StrimziKafkaContainer;
import io.strimzi.admin.systemtest.utils.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdminDeploymentManager {

    private static DockerClient client;
    private static String kafkaContId;
    public static final String NETWORK_NAME = "strimzi-admin-network";
    private static final Map<String, Stack<ThrowableRunner>> STORED_RESOURCES = new LinkedHashMap<>();
    private static final Map<String, StrimziKafkaContainer> KAFKA_CONTAINERS = new LinkedHashMap<>();
    private static final Map<String, Integer> ADMIN_PORTS = new LinkedHashMap<>();
    private static AdminDeploymentManager deploymentManager;
    protected static final Logger LOGGER = LogManager.getLogger(AdminDeploymentManager.class);

    public static synchronized AdminDeploymentManager getInstance() {
        if (deploymentManager == null) {
            deploymentManager = new AdminDeploymentManager();
        }
        return deploymentManager;
    }


    public void deployOauthStack(ExtensionContext testContext) throws Exception {
        LOGGER.info("*******************************************************");
        LOGGER.info("Deploying oauth stack for {}", testContext.getDisplayName());
        LOGGER.info("*******************************************************");
        try {
            createNetwork(testContext);
            deployKeycloak(testContext);
            deployZookeeper(testContext);
            deployKafka(testContext);
            deployAdminContainer(getKafkaIP() + ":9092", true, AdminDeploymentManager.NETWORK_NAME, testContext);
        } catch (Exception e) {
            e.printStackTrace();
            teardown(testContext);
        }
        LOGGER.info("*******************************************************");
        LOGGER.info("");
    }

    public void deployPlainStack(ExtensionContext testContext) throws Exception {
        LOGGER.info("Deploying strimzi kafka test container.");
        Network network = Network.newNetwork();
        StrimziKafkaContainer kafka = new StrimziKafkaContainer().withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withNetwork(network);
        kafka.start();
        String networkName = client.inspectNetworkCmd().withNetworkId(network.getId()).exec().getName();
        synchronized (this) {
            STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
            STORED_RESOURCES.get(testContext.getDisplayName()).push(kafka::stop);
            KAFKA_CONTAINERS.putIfAbsent(testContext.getDisplayName(), kafka);
        }
        LOGGER.info("_________________________________________");
        String kafkaIp = getKafkaIP(kafka.getContainerId(), networkName);
        deployAdminContainer(kafkaIp + ":9093", false, networkName, testContext);
    }


    private AdminDeploymentManager() {
        client = DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).build();
    }

    private void waitForAdminReady(int port) throws TimeoutException, InterruptedException {
        final int waitTimeout = 10;
        Vertx vertx = Vertx.vertx();
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < waitTimeout && !ready.get()) {
            HttpClient client = vertx.createHttpClient();

            client.request(HttpMethod.GET, port, "localhost", "/health/status")
                    .compose(req -> req.send().compose(HttpClientResponse::body))
                    .onComplete(httpClientRequestAsyncResult -> {
                        if (httpClientRequestAsyncResult.succeeded()
                                && httpClientRequestAsyncResult.result().toString().equals("{\"status\": \"OK\"}")) {
                            ready.set(true);
                        }
                    });
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }

    private void waitForKeycloakReady() throws TimeoutException, InterruptedException {
        final int waitTimeout = 60;
        Vertx vertx = Vertx.vertx();
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < waitTimeout && !ready.get()) {
            HttpClient client = vertx.createHttpClient();

            client.request(HttpMethod.GET, 8080, "localhost", "/auth/realms/demo")
                    .compose(req -> req.send().onComplete(res -> {
                        if (res.succeeded() && res.result().statusCode() == 200) {
                            ready.set(true);
                        }
                    }));
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }

    public void deployAdminContainer(String bootstrap, Boolean oauth, String networkName, ExtensionContext testContext) throws Exception {
        TestUtils.logDeploymentPhase("Deploying kafka admin api container");
        ExposedPort adminPort = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(adminPort, Ports.Binding.bindPort(8082));

        CreateContainerResponse contResp = client.createContainerCmd("strimzi-admin")
                .withExposedPorts(adminPort)
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPublishAllPorts(true)
                        .withNetworkMode(networkName))
                .withCmd("/opt/strimzi/run.sh -e KAFKA_ADMIN_BOOTSTRAP_SERVERS='" + bootstrap
                        + "' -e KAFKA_ADMIN_OAUTH_ENABLED='" + oauth + "' -e VERTXWEB_ENVIRONMENT='dev'").exec();
        String adminContId = contResp.getId();
        client.startContainerCmd(contResp.getId()).exec();
        int adminPublishedPort = Integer.parseInt(client.inspectContainerCmd(contResp.getId()).exec().getNetworkSettings()
                .getPorts().getBindings().get(adminPort)[0].getHostPortSpec());
        TestUtils.logDeploymentPhase("Waiting for admin to be up&running");
        waitForAdminReady(adminPublishedPort);
        synchronized (this) {
            STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
            STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(adminContId));
            ADMIN_PORTS.putIfAbsent(testContext.getDisplayName(), adminPublishedPort);
        }
    }

    public void deployKeycloak(ExtensionContext testContext) throws TimeoutException, InterruptedException {
        TestUtils.logDeploymentPhase("Deploying keycloak container");
        ExposedPort port = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(8080));
        CreateContainerResponse keycloakResp = client.createContainerCmd("strimzi-admin-keycloak")
                .withExposedPorts(port)
                .withName("keycloak")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)
                        .withNetworkMode(NETWORK_NAME)).exec();
        String keycloakContId = keycloakResp.getId();
        client.startContainerCmd(keycloakContId).exec();
        TestUtils.logDeploymentPhase("Deploying keycloak_import container");
        CreateContainerResponse keycloakImportResp = client.createContainerCmd("strimzi-admin-keycloak-import")
                .withName("keycloak_import")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                .withNetworkMode(NETWORK_NAME))
                .exec();
        String keycloakImportContId = keycloakImportResp.getId();
        client.startContainerCmd(keycloakImportContId).exec();
        TestUtils.logDeploymentPhase("Waiting for keycloak to be ready");
        waitForKeycloakReady();
        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(keycloakContId));
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(keycloakImportContId));
    }



    public void deployZookeeper(ExtensionContext testContext) {
        TestUtils.logDeploymentPhase("Deploying zookeeper container");
        ExposedPort port = ExposedPort.tcp(2181);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(2181));
        CreateContainerResponse zookeeperResp = client.createContainerCmd("strimzi-admin-zookeeper")
                .withExposedPorts(port)
                .withName("zookeeper")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)
                        .withNetworkMode(NETWORK_NAME)).exec();
        String zookeeperContId = zookeeperResp.getId();
        client.startContainerCmd(zookeeperContId).exec();
        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(zookeeperContId));
    }

    public void deployKafka(ExtensionContext testContext) {
        TestUtils.logDeploymentPhase("Deploying kafka container.");
        ExposedPort port = ExposedPort.tcp(9092);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(9092));
        CreateContainerResponse kafkaResp = client.createContainerCmd("strimzi-admin-kafka")
                .withExposedPorts(port)
                .withName("kafka")
                .withHostName("kafka")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)
                        .withNetworkMode(NETWORK_NAME)).exec();
        kafkaContId = kafkaResp.getId();
        client.startContainerCmd(kafkaContId).exec();
        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(kafkaContId));
    }

    public String getKafkaIP() {
        return client.inspectContainerCmd(kafkaContId).exec().getNetworkSettings().getNetworks()
                .get(AdminDeploymentManager.NETWORK_NAME).getIpAddress();
    }

    public String getKafkaIP(String kafkaId, String networkName) {
        return client.inspectContainerCmd(kafkaId).exec().getNetworkSettings().getNetworks()
                .get(networkName).getIpAddress();
    }


    public void createNetwork(ExtensionContext testContext) {
        String networkId = client.createNetworkCmd().withName(NETWORK_NAME).exec().getId();
        TestUtils.logDeploymentPhase("Created network with id " + networkId);
        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> {
            client.removeNetworkCmd(NETWORK_NAME).withNetworkId(networkId).exec();
        });
    }

    public void createRandNetwork(ExtensionContext testContext) {
        String networkId = client.createNetworkCmd().withName(testContext.getDisplayName()).exec().getId();
        TestUtils.logDeploymentPhase("Created network with id " + networkId);
        synchronized (this) {
            STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
            STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> {
                client.removeNetworkCmd(NETWORK_NAME).withNetworkId(networkId).exec();
            });
        }
    }

    public String getNetworkName(String networkId) {
        return client.listNetworksCmd().exec().stream()
                .filter(n -> n.getId().equals(networkId)).findFirst().get().getName();
    }

    public void teardown(ExtensionContext testContext) throws Exception {
        LOGGER.info("*******************************************************");
        LOGGER.info("Going to teardown all containers for {}", testContext.getDisplayName());
        LOGGER.info("*******************************************************");

        if (!STORED_RESOURCES.containsKey(testContext.getDisplayName()) || STORED_RESOURCES.get(testContext.getDisplayName()).isEmpty()) {
            LOGGER.info("No resources to delete for {}", testContext.getDisplayName());
        } else {
            while (STORED_RESOURCES.containsKey(testContext.getDisplayName()) && !STORED_RESOURCES.get(testContext.getDisplayName()).isEmpty()) {
                STORED_RESOURCES.get(testContext.getDisplayName()).pop().run();
            }
        }
        LOGGER.info("*******************************************************");
        LOGGER.info("");
        STORED_RESOURCES.remove(testContext.getDisplayName());
    }

    public DockerClient getClient() {
        return client;
    }

    private void deleteContainer(String contID) {
        LOGGER.info("Removing container with ID:  {}", contID);
        client.removeContainerCmd(contID).withForce(true).exec();
    }

    public StrimziKafkaContainer getKafkaContainer(ExtensionContext extensionContext) {
        return KAFKA_CONTAINERS.get(extensionContext.getDisplayName());
    }

    public int getAdminPort(ExtensionContext extensionContext) {
        return ADMIN_PORTS.get(extensionContext.getDisplayName());
    }
}
