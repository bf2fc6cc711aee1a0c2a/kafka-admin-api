package org.bf2.admin.kafka.systemtest.deployment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ContainerNetwork;
import io.strimzi.StrimziKafkaContainer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.systemtest.Environment;
import org.bf2.admin.kafka.systemtest.json.TokenModel;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.bf2.admin.kafka.systemtest.Environment.CONFIG;

@SuppressWarnings("resource")
public class DeploymentManager {

    protected static final Logger LOGGER = LogManager.getLogger(DeploymentManager.class);
    private static final String KAFKA_ALIAS = "kafka";

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

    private final boolean oauthEnabled;
    private final Network testNetwork;

    private GenericContainer<?> keycloakContainer;
    private KafkaContainer<?> kafkaContainer;
    private GenericContainer<?> adminContainer;

    public static DeploymentManager newInstance(boolean oauthEnabled) {
        return new DeploymentManager(oauthEnabled);
    }

    private DeploymentManager(boolean oauthEnabled) {
        this.oauthEnabled = oauthEnabled;
        this.testNetwork = Network.newNetwork();
    }

    private static String name(String prefix) {
        return prefix + '-' + UUID.randomUUID().toString();
    }

    public boolean isOauthEnabled() {
        return oauthEnabled;
    }

    public void shutdown() {
        stopAll(adminContainer, kafkaContainer, keycloakContainer);
    }

    private void stopAll(Startable... containers) {
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
        if (kafkaContainer == null) {
            if (oauthEnabled) {
                kafkaContainer = deployKafka();
            } else {
                kafkaContainer = deployStrimziKafka();
            }
        }

        return (GenericContainer<?>) kafkaContainer;
    }

    public void stopKafkaContainer() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }

    public GenericContainer<?> getAdminContainer() {
        if (adminContainer == null) {
            adminContainer = deployAdminContainer(kafkaContainer.getInternalBootstrapServers());
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
            return getAccessToken(vertx, userType)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public TokenModel getTokenNow(Vertx vertx, UserType userType) {
        try {
            return getAccessToken(vertx, userType.username)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Future<String> getAccessToken(Vertx vertx, UserType userType) {
        if (userType == UserType.INVALID) {
            return Future.succeededFuture(UUID.randomUUID().toString());
        }
        return getAccessToken(vertx, userType.username).map(TokenModel::getAccessToken);
    }

    private Future<TokenModel> getAccessToken(Vertx vertx, String username) {
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
            });
    }

    public String getExternalBootstrapServers() {
        if (kafkaContainer != null) {
            return this.kafkaContainer.getBootstrapServers();
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

    private GenericContainer<?> deployAdminContainer(String bootstrap) {
        LOGGER.info("Deploying Kafka Admin API container");

        Map<String, String> envMap = new HashMap<>();
        envMap.put("KAFKA_ADMIN_BOOTSTRAP_SERVERS", bootstrap);
        envMap.put("KAFKA_ADMIN_API_TIMEOUT_MS_CONFIG", "5000");
        envMap.put("KAFKA_ADMIN_REQUEST_TIMEOUT_MS_CONFIG", "4000");
        envMap.put("KAFKA_ADMIN_OAUTH_ENABLED", Boolean.toString(oauthEnabled));
        envMap.put("KAFKA_ADMIN_REPLICATION_FACTOR", "1");
        envMap.put("KAFKA_ADMIN_ACL_RESOURCE_OPERATIONS", CONFIG.getProperty("systemtests.kafka.admin.acl.resource-operations"));

        if (oauthEnabled) {
            envMap.put(KafkaAdminConfigRetriever.BROKER_TLS_ENABLED, "true");
            envMap.put(KafkaAdminConfigRetriever.BROKER_TRUSTED_CERT, encodeTLSConfig("/certs/ca.crt"));
            envMap.put("KAFKA_ADMIN_TLS_CERT", encodeTLSConfig("/certs/admin-tls-chain.crt"));
            envMap.put("KAFKA_ADMIN_TLS_KEY", encodeTLSConfig("/certs/admin-tls.key"));
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
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.admin-server"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("admin-server")))
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

        GenericContainer<?> container = new GenericContainer<>("quay.io/keycloak/keycloak:14.0.0")
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.keycloak"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("keycloak")))
                .withNetwork(testNetwork)
                .withNetworkAliases("keycloak")
                .withExposedPorts(8080)
                .withEnv(Map.of("KEYCLOAK_USER", "admin",
                        "KEYCLOAK_PASSWORD", "admin",
                        "PROXY_ADDRESS_FORWARDING", "true"))
                .withCopyFileToContainer(MountableFile.forClasspathResource("/keycloak/scripts/keycloak-ssl.cli"),
                        "/opt/jboss/keycloak/keycloak-ssl.cli")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/certs/keycloak.server.keystore.p12"),
                        "/opt/jboss/keycloak/standalone/configuration/certs/keycloak.server.keystore.p12")
                .withCommand("-Dkeycloak.profile.feature.upload_scripts=enabled")
                .waitingFor(Wait.forHttp("/auth/realms/demo").withStartupTimeout(Duration.ofMinutes(5)));

        LOGGER.info("Deploying keycloak_import container");

        new GenericContainer<>("quay.io/keycloak/keycloak:14.0.0")
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("keycloak-import")))
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(List.of("")))
                .withNetwork(testNetwork)
                .withEnv(Map.of("KEYCLOAK_HOST", "keycloak"))
                .withCopyFileToContainer(MountableFile.forClasspathResource("/keycloak-import/realms/authz-realm.json"),
                        "/opt/jboss/realms/authz-realm.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/keycloak-import/realms/demo-realm.json"),
                        "/opt/jboss/realms/demo-realm.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/keycloak-import/start.sh", 0755),
                        "/opt/jboss/start.sh")
                .withCommand("/opt/jboss/start.sh")
                .start();

        LOGGER.info("Waiting for keycloak container");
        container.start();
        return container;
    }

    private KafkaContainer<?> deployKafka() {
        LOGGER.info("Deploying Kafka container");

        Map<String, String> env = new HashMap<>();

        try (InputStream stream = getClass().getResourceAsStream("/kafka-oauth/env.properties")) {
            Properties envProps = new Properties();
            envProps.load(stream);
            envProps.keySet()
                .stream()
                .map(Object::toString)
                .forEach(key -> env.put(key, envProps.getProperty(key)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        var container = new KeycloakSecuredKafkaContainer(KAFKA_ALIAS, KafkaContainer.IMAGE_TAG)
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.oauth-kafka"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("oauth-kafka")))
                .withEnv(env)
                .withNetwork(testNetwork)
                .withNetworkAliases(KAFKA_ALIAS)
                .withClasspathResourceMapping("/certs/cluster.keystore.p12", "/opt/kafka/certs/cluster.keystore.p12", BindMode.READ_ONLY)
                .withClasspathResourceMapping("/certs/cluster.truststore.p12", "/opt/kafka/certs/cluster.truststore.p12", BindMode.READ_ONLY)
                .withClasspathResourceMapping("/kafka-oauth/config/", "/opt/kafka/config/strimzi/", BindMode.READ_ONLY)
                .withCopyFileToContainer(MountableFile.forClasspathResource("/kafka-oauth/scripts/functions.sh"), "/opt/kafka/functions.sh")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/kafka-oauth/scripts/simple_kafka_config.sh", 0755), "/opt/kafka/simple_kafka_config.sh")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/kafka-oauth/scripts/start.sh", 0755), "/opt/kafka/start.sh")
                .withCommand("/opt/kafka/start.sh");

        container.start();
        return container;
    }

    private KafkaContainer<?> deployStrimziKafka() {
        LOGGER.info("Deploying Strimzi Kafka container");

        class StrimziPlainKafkaContainer extends StrimziKafkaContainer
                implements KafkaContainer<StrimziKafkaContainer> {
            StrimziPlainKafkaContainer(String version) {
                super(version);
            }

            @Override
            public String getInternalBootstrapServers() {
                // Obtain the container's IP address on the test bridge network
                return getContainerInfo()
                        .getNetworkSettings()
                        .getNetworks()
                        .entrySet()
                        .stream()
                        .map(Map.Entry::getValue)
                        .filter(net -> net.getAliases().contains(KAFKA_ALIAS))
                        .findFirst()
                        .map(ContainerNetwork::getIpAddress)
                        .orElseThrow() + ":9093";
            }
        }

        var container = new StrimziPlainKafkaContainer(KafkaContainer.IMAGE_TAG)
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.plain-kafka"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("plain-kafka")))
                .withNetwork(testNetwork)
                .withNetworkAliases(KAFKA_ALIAS);

        container.start();
        return (KafkaContainer<?>) container;
    }

    private String encodeTLSConfig(String fileName) {
        String rawContent;

        try (InputStream stream = getClass().getResourceAsStream(fileName)) {
            rawContent = new BufferedReader(new InputStreamReader(stream))
                    .lines().collect(Collectors.joining("\n"));
            return Base64.getEncoder().encodeToString(rawContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
