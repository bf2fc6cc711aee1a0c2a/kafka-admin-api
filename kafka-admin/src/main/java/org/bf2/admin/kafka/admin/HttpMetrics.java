package org.bf2.admin.kafka.admin;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class HttpMetrics {
    private static final String FAILED_REQUESTS_COUNTER = "failed_requests";
    private static final String HTTP_STATUS_CODE = "status_code";

    @Inject
    PrometheusMeterRegistry meterRegistry;

    private Counter requestsCounter;
    private Counter openApiCounter;
    private Counter succeededRequestsCounter;
    private Counter deleteTopicCounter;
    private Counter createTopicCounter;
    private Counter updateTopicCounter;
    private Counter listTopicsCounter;
    private Counter describeTopicCounter;

    private Counter describeGroupCounter;
    private Counter resetGroupOffsetCounter;
    private Counter listGroupsCounter;
    private Counter deleteGroupCounter;

    private Timer listTopicRequestTimer;
    private Timer createTopicRequestTimer;
    private Timer updateTopicRequestTimer;
    private Timer deleteTopicRequestTimer;
    private Timer describeTopicRequestTimer;
    private Timer openApiRequestTimer;
    private Timer describeGroupRequestTimer;
    private Timer listGroupsRequestTimer;
    private Timer deleteGroupRequestTimer;
    private Timer resetGroupOffsetRequestTimer;

    private Counter getAclResourceOperationsCounter;
    private Timer getAclResourceOperationsRequestTimer;

    private Counter describeAclsCounter;
    private Timer describeAclsRequestTimer;

    private Counter createAclsCounter;
    private Timer createAclsRequestTimer;

    private Counter deleteAclsCounter;
    private Timer deleteAclsRequestTimer;

    public void init(@Observes StartupEvent event) {
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
