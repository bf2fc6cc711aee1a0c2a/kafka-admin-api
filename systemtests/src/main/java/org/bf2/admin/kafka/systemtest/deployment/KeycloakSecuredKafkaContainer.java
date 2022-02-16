package org.bf2.admin.kafka.systemtest.deployment;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.jboss.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

class KeycloakSecuredKafkaContainer extends GenericContainer<KeycloakSecuredKafkaContainer>
        implements KafkaContainer<KeycloakSecuredKafkaContainer> {

    protected static final Logger LOGGER = Logger.getLogger(KeycloakSecuredKafkaContainer.class);
    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final int KAFKA_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;

    private int kafkaExposedPort;
    private StringBuilder advertisedListeners;

    KeycloakSecuredKafkaContainer(String version) {
        super("quay.io/strimzi/kafka:" + version);
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

        LOGGER.infof("This is mapped port %s", kafkaExposedPort);

        advertisedListeners = new StringBuilder(getBootstrapServers());

        Collection<ContainerNetwork> cns = containerInfo.getNetworkSettings().getNetworks().values();

        for (ContainerNetwork cn : cns) {
            advertisedListeners.append("," + "REPLICATION://").append(cn.getIpAddress()).append(":9091");
        }

        LOGGER.infof("This is all advertised listeners for Kafka %s", advertisedListeners);

        String command = "#!/bin/bash \n";
        command += "bin/zookeeper-server-start.sh ./config/zookeeper.properties &\n";
        command += "/bin/bash /opt/kafka/start.sh"
                + " --override listeners=REPLICATION://0.0.0.0:9091,SECURE://0.0.0.0:" + KAFKA_PORT
                + " --override advertised.listeners=" + advertisedListeners.toString()
                + " --override zookeeper.connect=localhost:" + ZOOKEEPER_PORT
                + " --override listener.security.protocol.map=REPLICATION:SSL,SECURE:SASL_SSL"
                + " --override inter.broker.listener.name=REPLICATION\n";

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    @Override
    public String getBootstrapServers() {
        return String.format("SECURE://%s:%s", getContainerIpAddress(), kafkaExposedPort);
    }
}
