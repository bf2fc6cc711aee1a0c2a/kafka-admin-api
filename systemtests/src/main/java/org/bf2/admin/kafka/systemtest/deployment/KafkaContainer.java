package org.bf2.admin.kafka.systemtest.deployment;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;

interface KafkaContainer<SELF extends GenericContainer<SELF>> extends Startable, Container<SELF> {

    String getBootstrapServers();

    String getInternalBootstrapServers();

}
