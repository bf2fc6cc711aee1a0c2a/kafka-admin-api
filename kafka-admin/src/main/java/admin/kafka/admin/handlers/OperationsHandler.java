package admin.kafka.admin.handlers;

import admin.kafka.admin.HttpMetrics;
import admin.kafka.admin.KafkaAdminConfigRetriever;
import io.vertx.core.Vertx;

public interface OperationsHandler<T extends Object> {
    T createTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T describeTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T updateTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T deleteTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T listTopics(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T listGroups(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T describeGroup(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
    T deleteGroup(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics);
}
