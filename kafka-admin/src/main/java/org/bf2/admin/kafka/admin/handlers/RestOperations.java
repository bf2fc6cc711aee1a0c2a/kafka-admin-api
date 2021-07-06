package org.bf2.admin.kafka.admin.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Promise;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.AccessControlOperations;
import org.bf2.admin.kafka.admin.ConsumerGroupOperations;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.TopicOperations;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.admin.model.Types.PagedResponse;
import org.bf2.admin.kafka.admin.model.Types.TopicPartitionResetResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RestOperations extends CommonHandler implements OperationsHandler {

    private static final Logger log = LogManager.getLogger(RestOperations.class);

    /**
     * Default limit to the number of partitions that new topics may have configured.
     * This value may be overridden via environment variable <code>KAFKA_ADMIN_NUM_PARTITIONS_MAX</code>.
     */
    private static final String DEFAULT_NUM_PARTITIONS_MAX = "100";

    private final HttpMetrics httpMetrics;
    private final AccessControlOperations aclOperations;
    private final ObjectMapper mapper = new ObjectMapper();

    public RestOperations(KafkaAdminConfigRetriever config, HttpMetrics httpMetrics) {
        super(config);
        this.httpMetrics = httpMetrics;
        this.aclOperations = new AccessControlOperations(config);
    }

    @Override
    public void createTopic(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getCreateTopicCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getCreateTopicRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            Types.NewTopic inputTopic;
            Promise<Types.NewTopic> prom = Promise.promise();

            try {
                inputTopic = mapper.readValue(routingContext.getBody().getBytes(), Types.NewTopic.class);
            } catch (IOException e) {
                errorResponse(e, HttpResponseStatus.BAD_REQUEST, routingContext, httpMetrics, timer, requestTimerSample);
                log.error(e);
                prom.fail(e);
                return;
            }

            if (!internalTopicsAllowed() && inputTopic.getName().startsWith("__")) {
                prom.fail(new InvalidTopicException("Topic " + inputTopic.getName() + " cannot be created"));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
                return;
            }

            int maxPartitions = getNumPartitionsMax();

            if (!numPartitionsValid(inputTopic.getSettings(), maxPartitions)) {
                prom.fail(new InvalidTopicException(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                                                                  inputTopic.getName(),
                                                                  maxPartitions)));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
                return;
            }

            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                TopicOperations.createTopic(ac.result(), prom, inputTopic);
            }
            processResponse(prom, routingContext, HttpResponseStatus.CREATED, httpMetrics, timer, requestTimerSample);
        });
    }

    @Override
    public void describeTopic(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDescribeTopicCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

        String topicToDescribe = routingContext.pathParam("topicName");
        Promise<Types.Topic> prom = Promise.promise();
        if (topicToDescribe == null || topicToDescribe.isEmpty()) {
            prom.fail(new InvalidTopicException("Topic to describe has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
            return;
        }
        if (!internalTopicsAllowed() && topicToDescribe.startsWith("__")) {
            prom.fail(new InvalidTopicException("Topic " + topicToDescribe + " cannot be described"));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                TopicOperations.describeTopic(ac.result(), prom, topicToDescribe);
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void updateTopic(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getUpdateTopicCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getUpdateTopicRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

        Promise<Types.UpdatedTopic> prom = Promise.promise();
        String topicToUpdate = routingContext.pathParam("topicName");
        if (topicToUpdate == null || topicToUpdate.isEmpty()) {
            prom.fail(new InvalidTopicException("Topic to update has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
            return;
        }

        if (!internalTopicsAllowed() && topicToUpdate.startsWith("__")) {
            prom.fail(new InvalidTopicException("Topic " + topicToUpdate + " cannot be updated"));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                Types.UpdatedTopic updatedTopic;

                try {
                    updatedTopic = mapper.readValue(routingContext.getBody().getBytes(), Types.UpdatedTopic.class);
                } catch (IOException e) {
                    errorResponse(e, HttpResponseStatus.BAD_REQUEST, routingContext, httpMetrics, timer, requestTimerSample);
                    log.error(e);
                    prom.fail(e);
                    return;
                }

                updatedTopic.setName(topicToUpdate);

                int maxPartitions = getNumPartitionsMax();

                if (!numPartitionsLessThanMax(updatedTopic, maxPartitions)) {
                    prom.fail(new InvalidTopicException(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                            updatedTopic.getName(), maxPartitions)));
                    processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getCreateTopicRequestTimer(), requestTimerSample);
                    return;
                }
                TopicOperations.updateTopic(ac.result(), updatedTopic, prom);
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
        });

    }

    @Override
    public void deleteTopic(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDeleteTopicCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        String topicToDelete = routingContext.pathParam("topicName");
        Promise<List<String>> prom = Promise.promise();
        if (topicToDelete == null || topicToDelete.isEmpty()) {
            prom.fail(new InvalidTopicException("Topic to delete has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            return;
        }

        if (!internalTopicsAllowed() && topicToDelete.startsWith("__")) {
            prom.fail(new InvalidTopicException("Topic " + topicToDelete + " cannot be deleted"));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                TopicOperations.deleteTopics(ac.result(), Collections.singletonList(topicToDelete), prom);
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void listTopics(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        httpMetrics.getListTopicsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        String filter = routingContext.queryParams().get("filter");
        Types.SortDirectionEnum sortReverse = Types.SortDirectionEnum.fromString(routingContext.queryParams().get("order"));
        String sortKey = routingContext.queryParams().get("orderKey") == null ? "name" : routingContext.queryParams().get("orderKey");
        Types.OrderByInput orderBy = new Types.OrderByInput();
        orderBy.setField(sortKey);
        orderBy.setOrder(sortReverse);
        final Pattern pattern;
        Promise<Types.TopicList> prom = Promise.promise();
        if (filter != null && !filter.isEmpty()) {
            pattern = Pattern.compile(filter, Pattern.CASE_INSENSITIVE);
        } else {
            pattern = null;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                try {
                    TopicOperations.getTopicList(ac.result(), prom, pattern, parsePageRequest(routingContext), orderBy);
                } catch (NumberFormatException | InvalidRequestException e) {
                    prom.fail(e);
                    processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getListTopicRequestTimer(), requestTimerSample);
                    return;
                }
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getListTopicRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void listGroups(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        httpMetrics.getListGroupsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        String topicFilter = routingContext.queryParams().get("topic");
        String consumerGroupIdFilter = routingContext.queryParams().get("group-id-filter") == null ? "" : routingContext.queryParams().get("group-id-filter");

        Types.SortDirectionEnum sortReverse = Types.SortDirectionEnum.fromString(routingContext.queryParams().get("order"));
        String sortKey = routingContext.queryParams().get("orderKey") == null ? "name" : routingContext.queryParams().get("orderKey");
        Types.OrderByInput orderBy = new Types.OrderByInput();
        orderBy.setField(sortKey);
        orderBy.setOrder(sortReverse);

        final Pattern pattern;
        Promise<Types.TopicList> prom = Promise.promise();
        if (topicFilter != null && !topicFilter.isEmpty()) {
            pattern = Pattern.compile(topicFilter, Pattern.CASE_INSENSITIVE);
        } else {
            pattern = Pattern.compile(".*");
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                try {
                    ConsumerGroupOperations.getGroupList(ac.result(), prom, pattern, parsePageRequest(routingContext), consumerGroupIdFilter, orderBy);
                } catch (NumberFormatException | InvalidRequestException e) {
                    prom.fail(e);
                    processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getListGroupsRequestTimer(), requestTimerSample);
                    return;
                }
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getListGroupsRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void describeGroup(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDescribeGroupCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        String groupToDescribe = routingContext.pathParam("consumerGroupId");
        Promise<Types.Topic> prom = Promise.promise();
        if (!internalGroupsAllowed() && groupToDescribe.startsWith("strimzi")) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup " + groupToDescribe + " cannot be described."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            return;
        }
        if (groupToDescribe == null || groupToDescribe.isEmpty()) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup to describe has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeGroupRequestTimer(), requestTimerSample);
            return;
        }
        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                ConsumerGroupOperations.describeGroup(ac.result(), prom, Collections.singletonList(groupToDescribe));
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDescribeGroupRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void deleteGroup(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDeleteGroupCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        String groupToDelete = routingContext.pathParam("consumerGroupId");
        Promise<List<String>> prom = Promise.promise();
        if (!internalGroupsAllowed() && groupToDelete.startsWith("strimzi")) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup " + groupToDelete + " cannot be deleted"));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            return;
        }
        if (groupToDelete == null || groupToDelete.isEmpty()) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup to delete has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteGroupRequestTimer(), requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                ConsumerGroupOperations.deleteGroup(ac.result(), Collections.singletonList(groupToDelete), prom);
            }
            processResponse(prom, routingContext, HttpResponseStatus.NO_CONTENT, httpMetrics, httpMetrics.getDeleteGroupRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void resetGroupOffset(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getResetGroupOffsetCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getResetGroupOffsetRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        String groupToReset = routingContext.pathParam("consumerGroupId");

        Promise<List<TopicPartitionResetResult>> prom = Promise.promise();
        if (!internalGroupsAllowed() && groupToReset.startsWith("strimzi")) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup " + groupToReset + " cannot be reset."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
            return;
        }
        if (groupToReset == null || groupToReset.isEmpty()) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup to reset Offset has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, timer, requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                Types.ConsumerGroupOffsetResetParameters parameters;

                try {
                    parameters = mapper.readValue(routingContext.getBody().getBytes(), Types.ConsumerGroupOffsetResetParameters.class);
                } catch (IOException e) {
                    errorResponse(e, HttpResponseStatus.BAD_REQUEST, routingContext, httpMetrics, timer, requestTimerSample);
                    log.error(e);
                    prom.fail(e);
                    return;
                }

                parameters.setGroupId(groupToReset);
                ConsumerGroupOperations.resetGroupOffset(ac.result(), parameters, prom);
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getResetGroupOffsetRequestTimer(), requestTimerSample);
        });
    }

    @Override
    public void getAclResourceOperations(RoutingContext routingContext) {
        httpMetrics.getGetAclResourceOperationsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getGetAclResourceOperationsRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        Promise<String> promise = Promise.promise();
        promise.complete(this.kaConfig.getAclResourceOperations());
        processResponse(promise, routingContext, HttpResponseStatus.OK, httpMetrics, timer, requestTimerSample);
    }

    @Override
    public void describeAcls(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDescribeAclsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getDescribeAclsRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        Promise<PagedResponse<Types.AclBinding>> promise = Promise.promise();
        AdminClient client = AdminClient.create(acConfig);

        try {
            var filter = Types.AclBinding.fromQueryParams(routingContext.queryParams());
            aclOperations.getAcls(client, promise, filter, parsePageRequest(routingContext), null);
        } catch (Exception e) {
            promise.fail(e);
        } finally {
            // Use the Vertx client wrapper to close on worker thread
            KafkaAdminClient.create(routingContext.vertx(), client).close();
        }

        processResponse(promise, routingContext, HttpResponseStatus.OK, httpMetrics, timer, requestTimerSample);
    }

    @Override
    public void createAcl(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getCreateAclsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer timer = httpMetrics.getCreateAclsRequestTimer();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        AdminClient client = AdminClient.create(acConfig);

        try {
            Types.AclBinding binding =
                    mapper.readValue(routingContext.getBody().getBytes(), AccessControlOperations.TYPEREF_ACL_BINDING);
            Promise<Void> promise = Promise.promise();
            aclOperations.createAcl(client, promise, binding);
            processResponse(promise, routingContext, HttpResponseStatus.CREATED, httpMetrics, timer, requestTimerSample);
        } catch (IOException e) {
            errorResponse(e, HttpResponseStatus.BAD_REQUEST, routingContext, httpMetrics, timer, requestTimerSample);
            log.error(e);
        } catch (Exception e) {
            processFailure(e, routingContext, httpMetrics, timer, requestTimerSample);
        } finally {
            // Use the Vertx client wrapper to close on worker thread
            KafkaAdminClient.create(routingContext.vertx(), client).close();
        }
    }

    @Override
    public void deleteAcls(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getDeleteAclsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        Promise<PagedResponse<Types.AclBinding>> promise = Promise.promise();
        AdminClient client = AdminClient.create(acConfig);

        try {
            var filter = Types.AclBinding.fromQueryParams(routingContext.queryParams());
            aclOperations.deleteAcls(client, promise, filter);
        } catch (Exception e) {
            promise.fail(e);
        } finally {
            // Use the Vertx client wrapper to close on worker thread
            KafkaAdminClient.create(routingContext.vertx(), client).close();
        }

        processResponse(promise, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDeleteAclsRequestTimer(), requestTimerSample);
    }

    private boolean internalTopicsAllowed() {
        return System.getenv("KAFKA_ADMIN_INTERNAL_TOPICS_ENABLED") == null ? false : Boolean.valueOf(System.getenv("KAFKA_ADMIN_INTERNAL_TOPICS_ENABLED"));
    }

    private boolean internalGroupsAllowed() {
        return System.getenv("KAFKA_ADMIN_INTERNAL_CONSUMER_GROUPS_ENABLED") == null
                ? false : Boolean.valueOf(System.getenv("KAFKA_ADMIN_INTERNAL_CONSUMER_GROUPS_ENABLED"));
    }

    private boolean numPartitionsValid(Types.NewTopicInput settings, int maxPartitions) {
        int partitions = settings.getNumPartitions() != null ?
                settings.getNumPartitions() :
                    TopicOperations.DEFAULT_PARTITIONS;

        return partitions > 0 && partitions <= maxPartitions;
    }

    private boolean numPartitionsLessThanMax(Types.UpdatedTopic settings, int maxPartitions) {
        if (settings.getNumPartitions() != null) {
            return settings.getNumPartitions() < maxPartitions;
        } else {
            // user did not change the partitions
            return true;
        }
    }

    private int getNumPartitionsMax() {
        return Integer.parseInt(System.getenv().getOrDefault("KAFKA_ADMIN_NUM_PARTITIONS_MAX", DEFAULT_NUM_PARTITIONS_MAX));
    }

    public void errorHandler(RoutingContext routingContext) {
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        Promise<List<String>> prom = Promise.promise();
        prom.fail(routingContext.failure());
        processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getOpenApiRequestTimer(), requestTimerSample);
    }

    private Types.PageRequest parsePageRequest(RoutingContext routingContext) {
        Types.PageRequest pageRequest = new Types.PageRequest();

        boolean deprecatedPaginationUsed = false;
        if (routingContext.queryParams().get("offset") != null || routingContext.queryParams().get("limit") != null) {
            deprecatedPaginationUsed = true;
        }
        pageRequest.setDeprecatedFormat(deprecatedPaginationUsed);

        if (deprecatedPaginationUsed) {
            String offset = routingContext.queryParams().get("offset") == null ? "0" : routingContext.queryParams().get("offset");
            String limit = routingContext.queryParams().get("limit") == null ? "10" : routingContext.queryParams().get("limit");
            int offsetInt = Integer.parseInt(offset);
            int limitInt = Integer.parseInt(limit);
            pageRequest.setOffset(offsetInt);
            pageRequest.setLimit(limitInt);
        } else {
            String size = routingContext.queryParams().get("size") == null ? "10" : routingContext.queryParams().get("size");
            String page = routingContext.queryParams().get("page") == null ? "1" : routingContext.queryParams().get("page");

            int pageInt = Integer.parseInt(page);
            int sizeInt = Integer.parseInt(size);
            pageRequest.setPage(pageInt);
            pageRequest.setSize(sizeInt);

            if (sizeInt < 1 || pageInt < 1) {
                throw new InvalidRequestException("Size and page have to be positive integers.");
            }
        }

        return pageRequest;
    }
}
