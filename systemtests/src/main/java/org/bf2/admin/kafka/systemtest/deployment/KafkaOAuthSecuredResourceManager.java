package org.bf2.admin.kafka.systemtest.deployment;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

public class KafkaOAuthSecuredResourceManager implements QuarkusTestResourceLifecycleManager {

    DeploymentManager deployments;
    GenericContainer<?> kafkaContainer;

    @Override
    public Map<String, String> start() {
        deployments = DeploymentManager.newInstance(true);
        var keycloak = deployments.getKeycloakContainer();
        kafkaContainer = deployments.getKafkaContainer();
        String externalBootstrap = deployments.getExternalBootstrapServers();

        int kcPort = keycloak.getMappedPort(8080);

        return Map.of("%testoauth." + KafkaAdminConfigRetriever.BOOTSTRAP_SERVERS, externalBootstrap,
                      "%testoauth." + KafkaAdminConfigRetriever.OAUTH_JWKS_ENDPOINT_URI, String.format("http://localhost:%d/auth/realms/kafka-authz/protocol/openid-connect/certs", kcPort),
                      "%testoauth." + KafkaAdminConfigRetriever.OAUTH_TOKEN_ENDPOINT_URI, String.format("http://localhost:%d/auth/realms/kafka-authz/protocol/openid-connect/token", kcPort));
    }

    @Override
    public void stop() {
        deployments.shutdown();
    }

}
