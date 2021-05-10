package org.bf2.admin.kafka.systemtest.deployment;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Ports.Binding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.google.common.collect.Iterables;
import io.strimzi.StrimziKafkaContainer;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.utils.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AdminDeploymentManager {

    private static DockerClient client;
    private static String kafkaContId;
    public static final String NETWORK_NAME = "kafka-admin-network";
    private static final Map<String, Stack<ThrowableRunner>> STORED_RESOURCES = new LinkedHashMap<>();
    private static final Map<String, StrimziKafkaContainer> KAFKA_CONTAINERS = new LinkedHashMap<>();
    private static final Map<String, Integer> ADMIN_PORTS = new LinkedHashMap<>();
    private static final Map<String, Integer> MANAGEMENT_PORTS = new LinkedHashMap<>();
    private static AdminDeploymentManager deploymentManager;
    protected static final Logger LOGGER = LogManager.getLogger(AdminDeploymentManager.class);

    public static synchronized AdminDeploymentManager getInstance() {
        if (deploymentManager == null) {
            deploymentManager = new AdminDeploymentManager();
        }
        return deploymentManager;
    }

    private AdminDeploymentManager() {
        client = DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).build();
    }

    public void deployKeycloak(VertxTestContext vertxTestContext, ExtensionContext testContext) throws Exception {
        vertxTestContext.verify(() -> {
            LOGGER.info("*******************************************************");
            LOGGER.info("Deploying OAuth stack for {}", testContext.getDisplayName());
            LOGGER.info("*******************************************************");
            try {
                createNetwork(testContext);
                deployKeycloak(testContext, vertxTestContext);
            } catch (Exception e) {
                e.printStackTrace();
                teardown(testContext);
                vertxTestContext.failNow("Could not deploy OAuth stack");
            }
            LOGGER.info("*******************************************************");
            LOGGER.info("");
            vertxTestContext.completeNow();
            vertxTestContext.checkpoint();
        });
    }

    public void deployOauthStack(VertxTestContext vertxTestContext, ExtensionContext testContext) throws Exception {
        vertxTestContext.verify(() -> {
            LOGGER.info("*******************************************************");
            LOGGER.info("Deploying oauth stack for {}", testContext.getDisplayName());
            LOGGER.info("*******************************************************");
            try {
                deployZookeeper(testContext);
                deployKafka(testContext);
                deployAdminContainer("kafka:9092", true, false, AdminDeploymentManager.NETWORK_NAME, testContext, vertxTestContext);
            } catch (Exception e) {
                e.printStackTrace();
                teardown(testContext);
                vertxTestContext.failNow("Could not deploy OAUTH stack");
            }
            LOGGER.info("*******************************************************");
            LOGGER.info("");
            vertxTestContext.completeNow();
            vertxTestContext.checkpoint();
        });
    }

    public void deployPlainStack(VertxTestContext vertxTestContext, ExtensionContext testContext) throws Exception {
        vertxTestContext.verify(() -> {
            LOGGER.info("Deploying strimzi kafka test container.");
            Network network = Network.newNetwork();
            @SuppressWarnings("resource")
            StrimziKafkaContainer kafka = new StrimziKafkaContainer().withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                    .withNetwork(network);
            kafka.start();
            String networkName = client.inspectNetworkCmd().withNetworkId(network.getId()).exec().getName();
            synchronized (this) {
                STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
                STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> client.removeNetworkCmd(networkName).exec());
                STORED_RESOURCES.get(testContext.getDisplayName()).push(kafka::stop);
                KAFKA_CONTAINERS.putIfAbsent(testContext.getDisplayName(), kafka);
            }
            LOGGER.info("_________________________________________");
            String kafkaIp = getKafkaIP(kafka.getContainerId(), networkName);
            String className = Iterables.getLast(Arrays.stream(testContext.getTestClass()
                    .get().getName().split("[.]")).collect(Collectors.toList()));
            deployAdminContainer(kafkaIp + ":9093", false,
                    className.equals("RestEndpointInternalIT"), networkName, testContext, vertxTestContext);
            vertxTestContext.completeNow();
            vertxTestContext.checkpoint();
        });
    }

    private void waitForAdminReady(int port, VertxTestContext vertxTestContext) {
        Vertx vertx = Vertx.vertx();
        CircuitBreaker breaker = CircuitBreaker.create("admin-waiter", vertx, new CircuitBreakerOptions()
                .setTimeout(2000).setResetTimeout(3000).setMaxRetries(60)).retryPolicy(retryCount -> retryCount * 1000L);

        AtomicReference<String> health = new AtomicReference<>();
        breaker.<String>executeWithFallback(
            promise -> {
                vertx.createHttpClient().request(HttpMethod.GET, port, "localhost", "/health/status")
                        .compose(req -> req
                                .putHeader("Accept", "application/json")
                                .send().compose(resp -> resp
                                    .body()
                                    .map(Buffer::toString))
                        ).onComplete(promise);
            },
            t -> null
        ).onComplete(ar -> health.set(ar.result()));
        try {
            await().atMost(1, TimeUnit.MINUTES).untilAtomic(health, is(notNullValue()));
        } catch (Exception e) {
            vertxTestContext.failNow("Test failed during admin deployment");
        }
        assertThat(health.get()).isEqualTo("{\"status\": \"OK\"}");
        LOGGER.info("Admin container is ready");
        vertxTestContext.completeNow();
        vertxTestContext.checkpoint();
    }

    private void waitForKeycloakReady(VertxTestContext vertxTestContext) {
        Vertx vertx = Vertx.vertx();
        CircuitBreaker breaker = CircuitBreaker.create("admin-waiter", vertx, new CircuitBreakerOptions()
                .setTimeout(2000).setResetTimeout(3000).setMaxRetries(60)).retryPolicy(retryCount -> retryCount * 1000L);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        breaker.execute(future ->
                vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", "/auth/realms/demo").compose(req ->
                        req.send().compose(resp -> Future.succeededFuture(resp.statusCode()))
                ).onSuccess(sc -> {
                    if (sc != 200) {
                        future.fail("http error");
                    } else {
                        countDownLatch.countDown();
                        future.complete();
                    }
                }).onFailure(sc -> {
                    future.fail("http error");
                })
        );
        try {
            countDownLatch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            vertxTestContext.failNow("Test failed during keycloak deployment");
        }
        LOGGER.info("Keycloak ready");
        vertxTestContext.completeNow();
        vertxTestContext.checkpoint();
    }

    public void deployAdminContainer(String bootstrap, Boolean oauth, Boolean internal, String networkName, ExtensionContext testContext, VertxTestContext vertxTestContext) throws Exception {
        TestUtils.logDeploymentPhase("Deploying kafka admin api container");
        ExposedPort managementPort = ExposedPort.tcp(8080);
        ExposedPort resourcePort = ExposedPort.tcp(8443);

        List<ExposedPort> exposedPorts = new ArrayList<>(2);
        Ports boundPorts = new Ports();

        exposedPorts.add(managementPort);
        exposedPorts.add(resourcePort);

        List<String> env = new ArrayList<>(Arrays.asList(String.format("KAFKA_ADMIN_BOOTSTRAP_SERVERS=%s", bootstrap),
                                                         String.format("KAFKA_ADMIN_OAUTH_ENABLED=%s", oauth),
                                                         String.format("KAFKA_ADMIN_INTERNAL_TOPICS_ENABLED=%s", internal),
                                                         "KAFKA_ADMIN_REPLICATION_FACTOR=1",
                                                         String.format("KAFKA_ADMIN_TLS_CERT=%s", encodeTLSConfig("admin-tls-chain.crt")),
                                                         String.format("KAFKA_ADMIN_TLS_KEY=%s", encodeTLSConfig("admin-tls.key"))));

        Integer configuredDebugPort = Integer.getInteger("debugPort");

        if (configuredDebugPort != null) {
            ExposedPort debugPort = ExposedPort.tcp(configuredDebugPort);
            boundPorts.bind(debugPort, Ports.Binding.bindPort(configuredDebugPort));
            exposedPorts.add(debugPort);
            env.add(String.format("KAFKA_ADMIN_DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:%d", configuredDebugPort));
        }

        String customLogConfig = System.getProperty("customLogConfig");
        List<Mount> mounts;

        if (customLogConfig != null) {
            mounts = Collections.singletonList(new Mount()
                                               .withType(MountType.BIND)
                                               .withSource(customLogConfig)
                                               .withTarget("/opt/kafka-admin-api/custom-config/"));
        } else {
            mounts = Collections.emptyList();
        }

        CreateContainerResponse contResp = client.createContainerCmd("kafka-admin")
                .withExposedPorts(exposedPorts)
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPublishAllPorts(true)
                        .withPortBindings(boundPorts)
                        .withNetworkMode(networkName)
                        .withMounts(mounts))
                .withEnv(env)
                .exec();

        String adminContId = contResp.getId();

        synchronized (this) {
            STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
            STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(adminContId));
        }

        client.startContainerCmd(contResp.getId()).exec();

        Map<ExposedPort, Binding[]> portBindings = client.inspectContainerCmd(contResp.getId()).exec().getNetworkSettings()
                .getPorts().getBindings();

        int publishedManagementPort = Integer.parseInt(portBindings.get(managementPort)[0].getHostPortSpec());

        TestUtils.logDeploymentPhase("Waiting for admin to be up & running");
        waitForAdminReady(publishedManagementPort, vertxTestContext);

        int publishedResourcePort = Integer.parseInt(portBindings.get(resourcePort)[0].getHostPortSpec());

        synchronized (this) {
            ADMIN_PORTS.putIfAbsent(testContext.getDisplayName(), publishedResourcePort);
            MANAGEMENT_PORTS.putIfAbsent(testContext.getDisplayName(), publishedManagementPort);
        }
    }

    public void deployKeycloak(ExtensionContext testContext, VertxTestContext vertxTestContext) throws TimeoutException, InterruptedException {
        TestUtils.logDeploymentPhase("Deploying keycloak container");
        ExposedPort port = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(8080));
        CreateContainerResponse keycloakResp = client.createContainerCmd("kafka-admin-keycloak")
                .withExposedPorts(port)
                .withName("keycloak")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)
                        .withNetworkMode(NETWORK_NAME)).exec();
        String keycloakContId = keycloakResp.getId();

        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(keycloakContId));

        client.startContainerCmd(keycloakContId).exec();
        TestUtils.logDeploymentPhase("Deploying keycloak_import container");

        CreateContainerResponse keycloakImportResp = client.createContainerCmd("kafka-admin-keycloak-import")
                .withName("keycloak_import")
                .withLabels(Collections.singletonMap("test-ident", testContext.getUniqueId()))
                .withHostConfig(new HostConfig()
                .withNetworkMode(NETWORK_NAME))
                .exec();
        String keycloakImportContId = keycloakImportResp.getId();
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> deleteContainer(keycloakImportContId));

        client.startContainerCmd(keycloakImportContId).exec();
        TestUtils.logDeploymentPhase("Waiting for keycloak to be ready");
        waitForKeycloakReady(vertxTestContext);
    }

    public void deployZookeeper(ExtensionContext testContext) {
        TestUtils.logDeploymentPhase("Deploying zookeeper container");
        ExposedPort port = ExposedPort.tcp(2181);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(2181));
        CreateContainerResponse zookeeperResp = client.createContainerCmd("kafka-admin-zookeeper")
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
        CreateContainerResponse kafkaResp = client.createContainerCmd("kafka-admin-kafka")
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
        String networkId = client.listNetworksCmd()
            .withNameFilter(NETWORK_NAME)
            .exec()
            .stream()
            .map(com.github.dockerjava.api.model.Network::getName)
            .findFirst()
            .orElseGet(() -> {
                return client.createNetworkCmd().withName(NETWORK_NAME).exec().getId();
            });

        TestUtils.logDeploymentPhase("Using network with id " + networkId);
        STORED_RESOURCES.computeIfAbsent(testContext.getDisplayName(), k -> new Stack<>());
        STORED_RESOURCES.get(testContext.getDisplayName()).push(() -> {
            client.removeNetworkCmd(NETWORK_NAME).withNetworkId(networkId).exec();
        });
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

    private String encodeTLSConfig(String fileName) throws IOException {
        String rawContent = Files.readString(Path.of("docker", "certificates", fileName));
        return Base64.getEncoder().encodeToString(rawContent.getBytes(StandardCharsets.UTF_8));
    }

    private void deleteContainer(String contID) {
        LOGGER.info("Removing container with ID:  {}", contID);
        client.removeContainerCmd(contID).withForce(true).exec();
    }

    public StrimziKafkaContainer getKafkaContainer(ExtensionContext extensionContext) {
        return KAFKA_CONTAINERS.get(extensionContext.getDisplayName());
    }

    public int getManagementPort(ExtensionContext extensionContext) {
        return MANAGEMENT_PORTS.get(extensionContext.getDisplayName());
    }

    public int getAdminPort(ExtensionContext extensionContext) {
        Optional<ExtensionContext> context = Optional.of(extensionContext);

        while (context.isPresent()) {
            if (ADMIN_PORTS.containsKey(context.get().getDisplayName())) {
                return ADMIN_PORTS.get(context.get().getDisplayName());
            }
            context = context.get().getParent();
        }

        return -1;
    }
}
