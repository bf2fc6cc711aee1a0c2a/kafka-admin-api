package org.bf2.admin.kafka.systemtest.deployment;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.testcontainers.containers.GenericContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaOAuthSecuredResourceManager implements QuarkusTestResourceLifecycleManager {

    Map<String, String> initArgs;
    DeploymentManager deployments;
    GenericContainer<?> kafkaContainer;

    @Override
    public void init(Map<String, String> initArgs) {
        this.initArgs = Map.copyOf(initArgs);
    }

    @Override
    public Map<String, String> start() {
        deployments = DeploymentManager.newInstance(true);
        var keycloak = deployments.getKeycloakContainer();
        kafkaContainer = deployments.getKafkaContainer();
        String externalBootstrap = deployments.getExternalBootstrapServers();

        int kcPort = keycloak.getMappedPort(8080);
        String certPath;
        String keyPath;

        try {
            certPath = Path.of(getClass().getResource("/certs/admin-tls-chain.crt").toURI()).toString();
            keyPath = Path.of(getClass().getResource("/certs/admin-tls.key").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        String profile = "%" + initArgs.get("profile") + ".";

        return Map.of(profile + KafkaAdminConfigRetriever.BOOTSTRAP_SERVERS, externalBootstrap,
                      profile + KafkaAdminConfigRetriever.OAUTH_JWKS_ENDPOINT_URI, String.format("http://localhost:%d/auth/realms/kafka-authz/protocol/openid-connect/certs", kcPort),
                      profile + KafkaAdminConfigRetriever.OAUTH_TOKEN_ENDPOINT_URI, String.format("http://localhost:%d/auth/realms/kafka-authz/protocol/openid-connect/token", kcPort),
                      profile + "kafka.admin.tls.cert", certPath,
                      profile + "kafka.admin.tls.key", keyPath,
                      profile + KafkaAdminConfigRetriever.BROKER_TLS_ENABLED, "true",
                      profile + KafkaAdminConfigRetriever.BROKER_TRUSTED_CERT, encodeTLSConfig("/certs/ca.crt"));
    }

    @Override
    public void stop() {
        deployments.shutdown();
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
