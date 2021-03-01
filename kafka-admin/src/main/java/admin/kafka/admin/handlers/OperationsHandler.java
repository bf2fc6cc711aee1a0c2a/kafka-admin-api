package admin.kafka.admin.handlers;

import admin.kafka.admin.HttpMetrics;
import io.vertx.core.Vertx;

import java.util.Map;

public interface OperationsHandler<T extends Object> {
    T createTopic(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T describeTopic(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T updateTopic(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T deleteTopic(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T listTopics(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T listGroups(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T describeGroup(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
    T deleteGroup(Map<String, Object> acConfig, Vertx vertx, HttpMetrics httpMetrics);
}
