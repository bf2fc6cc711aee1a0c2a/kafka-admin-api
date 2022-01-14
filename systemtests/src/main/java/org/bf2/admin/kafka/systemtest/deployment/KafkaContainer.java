package org.bf2.admin.kafka.systemtest.deployment;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;

interface KafkaContainer<SELF extends GenericContainer<SELF>> extends Startable, Container<SELF> {

    public static final String IMAGE_TAG = "0.26.0-kafka-2.8.1";

    String getBootstrapServers();

    String getInternalBootstrapServers();

}
