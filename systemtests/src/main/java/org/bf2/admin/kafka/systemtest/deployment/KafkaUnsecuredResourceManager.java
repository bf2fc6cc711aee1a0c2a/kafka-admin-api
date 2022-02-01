package org.bf2.admin.kafka.systemtest.deployment;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

public class KafkaUnsecuredResourceManager implements QuarkusTestResourceLifecycleManager {

    DeploymentManager deployments;
    GenericContainer<?> kafkaContainer;

    @Override
    public Map<String, String> start() {
        deployments = DeploymentManager.newInstance(false);
        kafkaContainer = deployments.getKafkaContainer();
        String externalBootstrap = deployments.getExternalBootstrapServers();

        return Map.of("kafka.admin.bootstrap.servers", externalBootstrap,
                      "kafka.admin.oauth.enabled", "false",
                      "kafka.admin.replication.factor", "1");
    }

    @Override
    public void stop() {
        deployments.shutdown();
    }

}