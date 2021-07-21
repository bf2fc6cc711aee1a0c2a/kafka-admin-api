package org.bf2.admin.kafka.admin;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.micrometer.backends.BackendRegistries;

public class HttpMetrics {
    private static final String FAILED_REQUESTS_COUNTER = "failed_requests";
    private static final String HTTP_STATUS_CODE = "status_code";

    private final PrometheusMeterRegistry meterRegistry;
    private final Counter requestsCounter;
    private final Counter openApiCounter;
    private final Counter succeededRequestsCounter;
    private final Counter deleteTopicCounter;
    private final Counter createTopicCounter;
    private final Counter updateTopicCounter;
    private final Counter listTopicsCounter;
    private final Counter describeTopicCounter;

    private final Counter describeGroupCounter;
    private final Counter resetGroupOffsetCounter;
    private final Counter listGroupsCounter;
    private final Counter deleteGroupCounter;

    private final Timer listTopicRequestTimer;
    private final Timer createTopicRequestTimer;
    private final Timer updateTopicRequestTimer;
    private final Timer deleteTopicRequestTimer;
    private final Timer describeTopicRequestTimer;
    private final Timer openApiRequestTimer;
    private final Timer describeGroupRequestTimer;
    private final Timer listGroupsRequestTimer;
    private final Timer deleteGroupRequestTimer;
    private final Timer resetGroupOffsetRequestTimer;

    private final Counter getAclResourceOperationsCounter;
    private final Timer getAclResourceOperationsRequestTimer;

    private final Counter describeAclsCounter;
    private final Timer describeAclsRequestTimer;

    private final Counter createAclsCounter;
    private final Timer createAclsRequestTimer;

    private final Counter deleteAclsCounter;
    private final Timer deleteAclsRequestTimer;

    public HttpMetrics() {
        this.meterRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();

        requestsCounter = meterRegistry.counter("requests");
        openApiCounter = meterRegistry.counter("requests_openapi");
        /*
         * Status code 404 is a placeholder for defining the status_code label.
         */
        meterRegistry.counter(FAILED_REQUESTS_COUNTER, HTTP_STATUS_CODE, "404");
        succeededRequestsCounter = meterRegistry.counter("succeeded_requests");
        deleteTopicCounter = meterRegistry.counter("delete_topic_requests");
        createTopicCounter = meterRegistry.counter("create_topic_requests");
        updateTopicCounter = meterRegistry.counter("update_topic_requests");
        listTopicsCounter = meterRegistry.counter("list_topics_requests");
        describeTopicCounter = meterRegistry.counter("describe_topic_requests");

        listGroupsCounter = meterRegistry.counter("list_groups_requests");
        describeGroupCounter = meterRegistry.counter("get_group_requests");
        deleteGroupCounter = meterRegistry.counter("delete_group_requests");
        resetGroupOffsetCounter = meterRegistry.counter("reset_group_offset_requests");

        listTopicRequestTimer = meterRegistry.timer("list_topics_request_time");
        createTopicRequestTimer = meterRegistry.timer("create_topic_request_time");
        updateTopicRequestTimer = meterRegistry.timer("update_topic_request_time");
        deleteTopicRequestTimer = meterRegistry.timer("delete_topic_request_time");
        describeTopicRequestTimer = meterRegistry.timer("describe_topic_request_time");
        openApiRequestTimer = meterRegistry.timer("openapi_request_time");
        describeGroupRequestTimer = meterRegistry.timer("describe_group_request_time");
        listGroupsRequestTimer = meterRegistry.timer("list_groups_request_time");
        deleteGroupRequestTimer = meterRegistry.timer("delete_group_request_time");
        resetGroupOffsetRequestTimer = meterRegistry.timer("reset_group_offset_request_time");

        getAclResourceOperationsCounter = meterRegistry.counter("get_acl_resource_operations_requests");
        getAclResourceOperationsRequestTimer = meterRegistry.timer("get_acl_resource_operations_request_time");

        describeAclsCounter = meterRegistry.counter("describe_acls_requests");
        describeAclsRequestTimer = meterRegistry.timer("describe_acls_request_time");

        createAclsCounter = meterRegistry.counter("create_acls_requests");
        createAclsRequestTimer = meterRegistry.timer("create_acls_request_time");

        deleteAclsCounter = meterRegistry.counter("delete_acls_requests");
        deleteAclsRequestTimer = meterRegistry.timer("delete_acls_request_time");
    }

    public PrometheusMeterRegistry getRegistry() {
        return meterRegistry;
    }

    public Counter getFailedRequestsCounter(int httpStatusCode) {
        return getRegistry().counter(FAILED_REQUESTS_COUNTER, HTTP_STATUS_CODE, String.valueOf(httpStatusCode));
    }

    public Counter getRequestsCounter() {
        return requestsCounter;
    }

    public Counter getOpenApiCounter() {
        return openApiCounter;
    }

    public Counter getSucceededRequestsCounter() {
        return succeededRequestsCounter;
    }

    public Counter getCreateTopicCounter() {
        return createTopicCounter;
    }

    public Counter getDeleteTopicCounter() {
        return deleteTopicCounter;
    }

    public Counter getDescribeTopicCounter() {
        return describeTopicCounter;
    }

    public Counter getListTopicsCounter() {
        return listTopicsCounter;
    }

    public Counter getUpdateTopicCounter() {
        return updateTopicCounter;
    }

    public Counter getDeleteGroupCounter() {
        return deleteGroupCounter;
    }

    public Counter getDescribeGroupCounter() {
        return describeGroupCounter;
    }

    public Counter getListGroupsCounter() {
        return listGroupsCounter;
    }

    public Timer getListTopicRequestTimer() {
        return listTopicRequestTimer;
    }

    public Timer getCreateTopicRequestTimer() {
        return createTopicRequestTimer;
    }

    public Timer getUpdateTopicRequestTimer() {
        return updateTopicRequestTimer;
    }

    public Timer getDeleteTopicRequestTimer() {
        return deleteTopicRequestTimer;
    }

    public Timer getDescribeTopicRequestTimer() {
        return describeTopicRequestTimer;
    }

    public Timer getOpenApiRequestTimer() {
        return openApiRequestTimer;
    }

    public Timer getDescribeGroupRequestTimer() {
        return describeGroupRequestTimer;
    }

    public Timer getListGroupsRequestTimer() {
        return listGroupsRequestTimer;
    }

    public Timer getDeleteGroupRequestTimer() {
        return deleteGroupRequestTimer;
    }

    public Counter getResetGroupOffsetCounter() {
        return resetGroupOffsetCounter;
    }

    public Timer getResetGroupOffsetRequestTimer() {
        return resetGroupOffsetRequestTimer;
    }

    public Counter getGetAclResourceOperationsCounter() {
        return getAclResourceOperationsCounter;
    }

    public Timer getGetAclResourceOperationsRequestTimer() {
        return getAclResourceOperationsRequestTimer;
    }

    public Counter getDescribeAclsCounter() {
        return describeAclsCounter;
    }

    public Timer getDescribeAclsRequestTimer() {
        return describeAclsRequestTimer;
    }

    public Counter getCreateAclsCounter() {
        return createAclsCounter;
    }

    public Timer getCreateAclsRequestTimer() {
        return createAclsRequestTimer;
    }

    public Counter getDeleteAclsCounter() {
        return deleteAclsCounter;
    }

    public Timer getDeleteAclsRequestTimer() {
        return deleteAclsRequestTimer;
    }

}
