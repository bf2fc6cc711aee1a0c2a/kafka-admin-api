package org.bf2.admin.kafka.systemtest.deployment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.StrimziKafkaContainer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.json.TokenModel;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("resource")
public class DeploymentManager {

    protected static final Logger LOGGER = LogManager.getLogger(DeploymentManager.class);

    public enum UserType {
        OWNER("alice"),
        USER("susan"),
        OTHER("bob"),
        INVALID(null);

        String username;

        private UserType(String username) {
            this.username = username;
        }

        public String getUsername() {
            return username;
        }
    }

    private ExtensionContext testContext;
    private boolean oauthEnabled;
    private Network testNetwork;
    private GenericContainer<?> keycloakContainer;
    private GenericContainer<?> kafkaContainer;
    private GenericContainer<?> zookeeperContainer;
    private StrimziKafkaContainer strimziContainer;
    private GenericContainer<?> adminContainer;

    public static DeploymentManager newInstance(ExtensionContext testContext, boolean oauthEnabled) {
        return new DeploymentManager(testContext, oauthEnabled);
    }

    private DeploymentManager(ExtensionContext testContext, boolean oauthEnabled) {
        this.testContext = testContext;
        this.oauthEnabled = oauthEnabled;
        this.testNetwork = Network.newNetwork();
    }

    public void shutdown() {
        stopAll(adminContainer,
                strimziContainer,
                kafkaContainer,
                zookeeperContainer,
                keycloakContainer);
    }

    private void stopAll(GenericContainer<?>... containers) {
        for (var container : containers) {
            if (container != null) {
                container.stop();
            }
        }
    }

    public GenericContainer<?> getKeycloakContainer() {
        if (keycloakContainer == null) {
            keycloakContainer = deployKeycloak();
        }

        return keycloakContainer;
    }

    public void stopKeycloakContainer() {
        if (keycloakContainer != null) {
            keycloakContainer.stop();
            keycloakContainer = null;
        }
    }

    public GenericContainer<?> getKafkaContainer() {
        if (oauthEnabled) {
            if (kafkaContainer == null) {
                zookeeperContainer = deployZookeeper();
                kafkaContainer = deployKafka();
            }

            return kafkaContainer;
        }

        if (strimziContainer == null) {
            strimziContainer = deployStrimziKafka();
        }

        return strimziContainer;
    }

    public void stopKafkaContainer() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
            zookeeperContainer.stop();
            zookeeperContainer = null;
        }

        if (strimziContainer != null) {
            strimziContainer.stop();
            strimziContainer = null;
        }
    }

    public GenericContainer<?> getAdminContainer() {
        if (adminContainer == null) {
            boolean allowInternal = testContext.getTestClass()
                .map(Class::getSimpleName)
                .map("RestEndpointInternalIT"::equals)
                .orElse(false);

            String port = (this.strimziContainer != null) ? "9093" : "9092";
            adminContainer = deployAdminContainer("kafka:" + port, allowInternal);
        }

        return adminContainer;
    }

    public void stopAdminContainer() {
        if (adminContainer != null) {
            adminContainer.stop();
            adminContainer = null;
        }
    }

    public AdminClient createKafkaAdmin() {
        return AdminClient.create(RequestUtils.getKafkaAdminConfig(getExternalBootstrapServers()));
    }

    public AdminClient createKafkaAdmin(String accessToken) {
        return AdminClient.create(ClientsConfig.getAdminConfigOauth(accessToken, getExternalBootstrapServers()));
    }

    public String getAccessTokenNow(Vertx vertx, UserType userType) {
        try {
            return getAccessToken(vertx, userType).toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Future<String> getAccessToken(Vertx vertx, UserType userType) {
        if (userType == UserType.INVALID) {
            return Future.succeededFuture(UUID.randomUUID().toString());
        }
        return getAccessToken(vertx, userType.username);
    }

    public Future<String> getAccessToken(Vertx vertx, String username) {
        final String payload = String.format("grant_type=password&username=%1$s&password=%1$s-password&client_id=kafka-cli", username);
        int port = keycloakContainer.getMappedPort(8080);

        return vertx.createHttpClient()
            .request(HttpMethod.POST, port, "localhost", "/auth/realms/kafka-authz/protocol/openid-connect/token")
            .map(req ->
                req.putHeader("Host", "keycloak:8080")
                   .putHeader("Content-Type", "application/x-www-form-urlencoded"))
            .compose(req -> req.send(payload))
            .compose(HttpClientResponse::body)
            .map(buffer -> {
                try {
                    return new ObjectMapper().readValue(buffer.toString(), TokenModel.class);
                } catch (JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            })
            .map(TokenModel::getAccessToken);
    }

    public String getBootstrapServers() {
        if (strimziContainer != null) {
            return String.format("%s:%d", strimziContainer.getContainerIpAddress(), 9093);
        }

        if (kafkaContainer != null) {
            return String.format("localhost:%d", kafkaContainer.getMappedPort(9092));
        }

        return null;
    }

    public String getExternalBootstrapServers() {
        if (strimziContainer != null) {
            return strimziContainer.getBootstrapServers();
        }

        if (kafkaContainer != null) {
            return String.format("localhost:%d", kafkaContainer.getMappedPort(9092));
        }

        return null;
    }

    public int getAdminServerPort() {
        if (adminContainer != null) {
            return adminContainer.getMappedPort(oauthEnabled ? 8443 : 8080);
        }

        throw new IllegalStateException("Admin server not running");
    }

    public int getAdminServerManagementPort() {
        if (adminContainer != null) {
            return adminContainer.getMappedPort(9990);
        }

        throw new IllegalStateException("Admin server not running");
    }

    private GenericContainer<?> deployAdminContainer(String bootstrap, boolean internal) {
        LOGGER.info("Deploying Kafka Admin API container");

        Map<String, String> envMap = new HashMap<>();
        envMap.put("KAFKA_ADMIN_BOOTSTRAP_SERVERS", bootstrap);
        envMap.put("KAFKA_ADMIN_OAUTH_ENABLED", Boolean.toString(oauthEnabled));
        envMap.put("KAFKA_ADMIN_INTERNAL_TOPICS_ENABLED", Boolean.toString(internal));
        envMap.put("KAFKA_ADMIN_REPLICATION_FACTOR", "1");
        envMap.put("KAFKA_ADMIN_ACL_RESOURCE_OPERATIONS", "{ \"cluster\": [ \"describe\", \"alter\" ], \"group\": [ \"all\", \"delete\", \"describe\", \"read\" ], \"topic\": [ \"all\", \"alter\", \"alter_configs\", \"create\", \"delete\", \"describe\", \"describe_configs\", \"read\", \"write\" ], \"transactional_id\": [ \"all\", \"describe\", \"write\" ] }");

        if (oauthEnabled) {
            envMap.put("KAFKA_ADMIN_TLS_CERT", encodeTLSConfig("admin-tls-chain.crt"));
            envMap.put("KAFKA_ADMIN_TLS_KEY", encodeTLSConfig("admin-tls.key"));
            envMap.put("KAFKA_ADMIN_OAUTH_JWKS_ENDPOINT_URI", "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/certs");
            envMap.put("KAFKA_ADMIN_OAUTH_VALID_ISSUER_URI", "http://keycloak:8080/auth/realms/kafka-authz");
            envMap.put("KAFKA_ADMIN_OAUTH_TOKEN_ENDPOINT_URI", "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/token");
        }

        class KafkaAdminServerContainer extends GenericContainer<KafkaAdminServerContainer> {
            KafkaAdminServerContainer() {
                super("kafka-admin");
            }
            @Override
            public void addFixedExposedPort(int hostPort, int containerPort) {
                super.addFixedExposedPort(hostPort, containerPort);
            }
        }

        KafkaAdminServerContainer container = new KafkaAdminServerContainer()
                .withNetwork(testNetwork)
                .withExposedPorts(oauthEnabled ? 8443 : 8080, 9990)
                .withEnv(envMap)
                .waitingFor(Wait.forHttp("/health/status").forPort(9990));

        Integer configuredDebugPort = Integer.getInteger("debugPort");

        if (configuredDebugPort != null) {
            container.addExposedPort(configuredDebugPort);
            container.addFixedExposedPort(configuredDebugPort, configuredDebugPort);
            container.addEnv("KAFKA_ADMIN_DEBUG", String.format("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:%d", configuredDebugPort));
        }

        String customLogConfig = System.getProperty("customLogConfig");

        if (customLogConfig != null) {
            container.addFileSystemBind(customLogConfig, "/opt/kafka-admin-api/custom-config/", BindMode.READ_ONLY);
        }

        container.start();
        return container;
    }

    public GenericContainer<?> deployKeycloak() {
        LOGGER.info("Deploying keycloak container");

        GenericContainer<?> container = new GenericContainer<>("kafka-admin-keycloak")
                .withNetwork(testNetwork)
                .withNetworkAliases("keycloak")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/auth/realms/demo"));

        LOGGER.info("Deploying keycloak_import container");

        new GenericContainer<>("kafka-admin-keycloak-import")
            .withNetwork(testNetwork)
            .start();

        LOGGER.info("Waiting for keycloak container");
        container.start();
        return container;
    }

    private GenericContainer<?> deployZookeeper() {
        LOGGER.info("Deploying zookeeper container");
        GenericContainer<?> container = new GenericContainer<>("kafka-admin-zookeeper")
                .withNetwork(testNetwork)
                .withNetworkAliases("zookeeper")
                .withExposedPorts(2181);

        container.start();
        return container;
    }

    private GenericContainer<?> deployKafka() {
        LOGGER.info("Deploying Kafka container");
        class KafkaContainer extends GenericContainer<KafkaContainer> {
            KafkaContainer() {
                super("kafka-admin-kafka");
            }
            @Override
            public void addFixedExposedPort(int hostPort, int containerPort) {
                super.addFixedExposedPort(hostPort, containerPort);
            }
        }

        KafkaContainer container = new KafkaContainer()
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka")
                .withExposedPorts(9092);

        // TODO: Should not need to fix the exposed port - update tests
        container.addFixedExposedPort(9092, 9092);
        container.start();
        return container;
    }

    private StrimziKafkaContainer deployStrimziKafka() {
        LOGGER.info("Deploying Strimzi Kafka container");
        class StrimziKafkaFixedContainer extends StrimziKafkaContainer {
            public StrimziKafkaFixedContainer(String version) {
                super(version);
            }
            @Override
            public void addFixedExposedPort(int hostPort, int containerPort) {
                super.addFixedExposedPort(hostPort, containerPort);
            }
        }

        StrimziKafkaFixedContainer container = (StrimziKafkaFixedContainer)
                new StrimziKafkaFixedContainer("latest-kafka-2.7.0")
                    .withNetwork(testNetwork)
                    .withNetworkAliases("kafka");

        // TODO: Should not need to fix the exposed port - update tests
        //container.addFixedExposedPort(9092, 9092);
        container.start();
        return container;
    }

    private String encodeTLSConfig(String fileName) {
        String rawContent;

        try {
            rawContent = Files.readString(Path.of("docker", "certificates", fileName));
            return Base64.getEncoder().encodeToString(rawContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
