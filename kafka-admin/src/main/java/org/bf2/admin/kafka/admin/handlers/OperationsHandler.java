package org.bf2.admin.kafka.admin.handlers;

import io.vertx.ext.web.RoutingContext;

public interface OperationsHandler {
    void createTopic(RoutingContext routingContext);
    void describeTopic(RoutingContext routingContext);
    void updateTopic(RoutingContext routingContext);
    void deleteTopic(RoutingContext routingContext);
    void listTopics(RoutingContext routingContext);
    void listGroups(RoutingContext routingContext);
    void describeGroup(RoutingContext routingContext);
    void deleteGroup(RoutingContext routingContext);
    void resetGroupOffset(RoutingContext routingContext);
}
