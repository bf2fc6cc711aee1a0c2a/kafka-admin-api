package org.bf2.admin.kafka.systemtest.deployment;

import io.strimzi.test.container.StrimziKafkaContainer;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.systemtest.Environment;
import org.jboss.logging.Logger;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.bf2.admin.kafka.systemtest.Environment.CONFIG;

@SuppressWarnings("resource")
public class DeploymentManager {

    protected static final Logger LOGGER = Logger.getLogger(DeploymentManager.class);

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
            adminContainer = deployAdminContainer(kafkaContainer.getBootstrapServers());
        }

        return adminContainer;
    }

    public void stopAdminContainer() {
        if (adminContainer != null) {
            adminContainer.stop();
            adminContainer = null;
        }
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
            return adminContainer.getMappedPort(8080);
        }

        throw new IllegalStateException("Admin server not running");
    }

    private GenericContainer<?> deployAdminContainer(String bootstrap) {
        LOGGER.info("Deploying Kafka Admin API container");

        Map<String, String> envMap = new HashMap<>();
        envMap.put("KAFKA_ADMIN_BOOTSTRAP_SERVERS", bootstrap);
        envMap.put("KAFKA_ADMIN_API_TIMEOUT_MS_CONFIG", "7000");
        envMap.put("KAFKA_ADMIN_REQUEST_TIMEOUT_MS_CONFIG", "6000");
        envMap.put("KAFKA_ADMIN_OAUTH_ENABLED", Boolean.toString(oauthEnabled));
        envMap.put("QUARKUS_SMALLRYE_JWT_ENABLED", Boolean.toString(oauthEnabled));
        envMap.put("KAFKA_ADMIN_REPLICATION_FACTOR", "1");
        envMap.put("KAFKA_ADMIN_ACL_RESOURCE_OPERATIONS", CONFIG.getProperty("systemtests.kafka.admin.acl.resource-operations"));

        final String pathCert = "/certs/admin-tls-chain.crt";
        final String pathKey = "/certs/admin-tls.key";

        if (oauthEnabled) {
            envMap.put(KafkaAdminConfigRetriever.BROKER_TLS_ENABLED, "true");
            envMap.put(KafkaAdminConfigRetriever.BROKER_TRUSTED_CERT, encodeTLSConfig("/certs/ca.crt"));
            envMap.put("KAFKA_ADMIN_TLS_CERT", pathCert);
            envMap.put("KAFKA_ADMIN_TLS_KEY", pathKey);
            envMap.put("KAFKA_ADMIN_OAUTH_JWKS_ENDPOINT_URI", "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/certs");
            envMap.put("KAFKA_ADMIN_OAUTH_VALID_ISSUER_URI", "http://keycloak:8080/auth/realms/kafka-authz");
            envMap.put("KAFKA_ADMIN_OAUTH_TOKEN_ENDPOINT_URI", "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/token");
        }

        class KafkaAdminServerContainer extends GenericContainer<KafkaAdminServerContainer> {
            KafkaAdminServerContainer() {
                super(System.getProperty("kafka-admin.image", "localhost/bf2/kafka-admin:latest"));
            }
            @Override
            public void addFixedExposedPort(int hostPort, int containerPort) {
                super.addFixedExposedPort(hostPort, containerPort);
            }
        }

        List<Integer> exposedPorts = new ArrayList<>(2);
        exposedPorts.add(8080); // health check
        if (oauthEnabled) {
            exposedPorts.add(8443);
        }

        KafkaAdminServerContainer container = new KafkaAdminServerContainer()
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.admin-server"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("admin-server")))
                .withNetwork(testNetwork)
                .withExposedPorts(exposedPorts.toArray(Integer[]::new))
                .withEnv(envMap)
                .waitingFor(Wait.forHttp("/health/started").forPort(8080));

        if (oauthEnabled) {
            container
                .withClasspathResourceMapping(pathCert, pathCert, BindMode.READ_ONLY)
                .withClasspathResourceMapping(pathKey, pathKey, BindMode.READ_ONLY);
        }

        Integer configuredDebugPort = Integer.getInteger("debugPort");

        if (configuredDebugPort != null) {
            //container.addExposedPort(configuredDebugPort);
            container.addFixedExposedPort(configuredDebugPort, 5005);
            container.addEnv("JAVA_DEBUG", "true");
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
        String imageName = System.getProperty("keycloak.image");

        GenericContainer<?> container = new GenericContainer<>(imageName)
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

        new GenericContainer<>(imageName)
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

        String imageTag = System.getProperty("strimzi-kafka.tag");

        var container = new KeycloakSecuredKafkaContainer(imageTag)
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.oauth-kafka"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("oauth-kafka")))
                .withEnv(env)
                .withNetwork(testNetwork)
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
        }

        String imageTag = System.getProperty("strimzi-kafka.tag");

        var container = new StrimziPlainKafkaContainer(imageTag)
                .withLabels(Collections.singletonMap("test-ident", Environment.TEST_CONTAINER_LABEL))
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("systemtests.plain-kafka"), true))
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name("plain-kafka")))
                .withKafkaConfigurationMap(Map.of("auto.create.topics.enable", "false"))
                .withNetwork(testNetwork);

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
