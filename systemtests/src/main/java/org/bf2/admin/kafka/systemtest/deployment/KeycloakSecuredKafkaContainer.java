package org.bf2.admin.kafka.systemtest.deployment;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

class KeycloakSecuredKafkaContainer extends GenericContainer<KeycloakSecuredKafkaContainer>
        implements KafkaContainer<KeycloakSecuredKafkaContainer> {

    protected static final Logger LOGGER = LogManager.getLogger(KeycloakSecuredKafkaContainer.class);
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;

    private int kafkaExposedPort;
    private StringBuilder advertisedListeners;

    KeycloakSecuredKafkaContainer() {
        super("kafka-admin-kafka");
        withExposedPorts(KAFKA_PORT);
    }

    @Override
    public void addFixedExposedPort(int hostPort, int containerPort) {
        super.addFixedExposedPort(hostPort, containerPort);
    }

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        kafkaExposedPort = getMappedPort(KAFKA_PORT);

        LOGGER.info("This is mapped port {}", kafkaExposedPort);

        advertisedListeners = new StringBuilder(getBootstrapServers());

        Collection<ContainerNetwork> cns = containerInfo.getNetworkSettings().getNetworks().values();

        for (ContainerNetwork cn : cns) {
            advertisedListeners.append("," + "REPLICATION://").append(cn.getIpAddress()).append(":9091");
            advertisedListeners.append("," + "INTERNAL://").append(cn.getIpAddress()).append(":9093");
        }

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners);

        String command = "#!/bin/bash \n";
        command += "bin/zookeeper-server-start.sh config/zookeeper.properties &\n";
        command += "/bin/bash /opt/kafka/start.sh"
                + " --override listeners=REPLICATION://0.0.0.0:9091,INTERNAL://0.0.0.0:9093,PLAINTEXT://0.0.0.0:" + KAFKA_PORT
                + " --override advertised.listeners=" + advertisedListeners.toString()
                + " --override zookeeper.connect=localhost:" + ZOOKEEPER_PORT
                + " --override listener.security.protocol.map=REPLICATION:SSL,PLAINTEXT:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT"
                + " --override inter.broker.listener.name=REPLICATION\n";

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    @Override
    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), kafkaExposedPort);
    }
}
