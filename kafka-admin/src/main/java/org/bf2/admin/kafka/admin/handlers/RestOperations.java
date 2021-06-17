package org.bf2.admin.kafka.admin.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.ConsumerGroupOperations;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.TopicOperations;
import org.bf2.admin.kafka.admin.model.Types;

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

    public RestOperations(KafkaAdminConfigRetriever config, HttpMetrics httpMetrics) {
        super(config);
        this.httpMetrics = httpMetrics;
    }

    @Override
    public void createTopic(RoutingContext routingContext) {
        Map<String, Object> acConfig = routingContext.get(ADMIN_CLIENT_CONFIG);
        httpMetrics.getCreateTopicCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            Types.NewTopic inputTopic = new Types.NewTopic();
            Promise<Types.NewTopic> prom = Promise.promise();
            ObjectMapper mapper = new ObjectMapper();
            try {
                inputTopic = mapper.readValue(routingContext.getBody().getBytes(), Types.NewTopic.class);
            } catch (IOException e) {
                routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                JsonObject jsonObject = new JsonObject();
                jsonObject.put("code", routingContext.response().getStatusCode());
                jsonObject.put("error", e.getMessage());
                routingContext.response().end(jsonObject.toBuffer());
                log.error(e);
                prom.fail(e);
                httpMetrics.getFailedRequestsCounter(HttpResponseStatus.BAD_REQUEST.code()).increment();
                requestTimerSample.stop(httpMetrics.getCreateTopicRequestTimer());
                return;
            }

            if (!internalTopicsAllowed() && inputTopic.getName().startsWith("__")) {
                prom.fail(new InvalidTopicException("Topic " + inputTopic.getName() + " cannot be created"));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getCreateTopicRequestTimer(), requestTimerSample);
                return;
            }

            int maxPartitions = getNumPartitionsMax();

            if (!numPartitionsValid(inputTopic.getSettings(), maxPartitions)) {
                prom.fail(new InvalidTopicException(String.format("Number of partitions for topic %s must between 1 and %d (inclusive)",
                                                                  inputTopic.getName(),
                                                                  maxPartitions)));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getCreateTopicRequestTimer(), requestTimerSample);
                return;
            }

            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                TopicOperations.createTopic(ac.result(), prom, inputTopic);
            }
            processResponse(prom, routingContext, HttpResponseStatus.CREATED, httpMetrics, httpMetrics.getCreateTopicRequestTimer(), requestTimerSample);
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
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

        Promise<Types.UpdatedTopic> prom = Promise.promise();
        String topicToUpdate = routingContext.pathParam("topicName");
        if (topicToUpdate == null || topicToUpdate.isEmpty()) {
            prom.fail(new InvalidTopicException("Topic to update has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
            return;
        }

        if (!internalTopicsAllowed() && topicToUpdate.startsWith("__")) {
            prom.fail(new InvalidTopicException("Topic " + topicToUpdate + " cannot be updated"));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                Types.UpdatedTopic updatedTopic = new Types.UpdatedTopic();
                ObjectMapper mapper = new ObjectMapper();
                try {
                    updatedTopic = mapper.readValue(routingContext.getBody().getBytes(), Types.UpdatedTopic.class);
                    updatedTopic.setName(topicToUpdate);
                } catch (IOException e) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.put("code", routingContext.response().getStatusCode());
                    jsonObject.put("error", e.getMessage());
                    routingContext.response().end(jsonObject.toBuffer());
                    requestTimerSample.stop(httpMetrics.getUpdateTopicRequestTimer());
                    httpMetrics.getFailedRequestsCounter(HttpResponseStatus.BAD_REQUEST.code()).increment();
                    prom.fail(e);
                    log.error(e);
                    return;
                }
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
        String size = routingContext.queryParams().get("size") == null ? "10" : routingContext.queryParams().get("size");
        String page = routingContext.queryParams().get("page") == null ? "1" : routingContext.queryParams().get("page");
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
                    int pageInt = Integer.parseInt(page);
                    int sizeInt = Integer.parseInt(size);
                    if (sizeInt < 1 || pageInt < 1) {
                        throw new InvalidRequestException("Size and page have to be positive integers.");
                    }
                    Types.PageRequest pageRequest = new Types.PageRequest();
                    pageRequest.setPage(pageInt);
                    pageRequest.setSize(sizeInt);

                    TopicOperations.getTopicList(ac.result(), prom, pattern, pageRequest, orderBy);
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
        String size = routingContext.queryParams().get("size") == null ? "10" : routingContext.queryParams().get("size");
        String page = routingContext.queryParams().get("page") == null ? "1" : routingContext.queryParams().get("page");

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
                    int pageInt = Integer.parseInt(page);
                    int sizeInt = Integer.parseInt(size);
                    if (sizeInt < 1 || pageInt < 1) {
                        throw new InvalidRequestException("Size and page have to be positive integers.");
                    }
                    Types.PageRequest pageRequest = new Types.PageRequest();
                    pageRequest.setPage(pageInt);
                    pageRequest.setSize(sizeInt);
                    ConsumerGroupOperations.getGroupList(ac.result(), prom, pattern, pageRequest, consumerGroupIdFilter, orderBy);
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
        httpMetrics.getRequestsCounter().increment();
        httpMetrics.getRequestsCounter().increment();
        Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
        String groupToReset = routingContext.pathParam("consumerGroupId");

        Promise<List<String>> prom = Promise.promise();
        if (!internalGroupsAllowed() && groupToReset.startsWith("strimzi")) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup " + groupToReset + " cannot be reset."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            return;
        }
        if (groupToReset == null || groupToReset.isEmpty()) {
            prom.fail(new InvalidConsumerGroupException("ConsumerGroup to reset Offset has not been specified."));
            processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getResetGroupOffsetRequestTimer(), requestTimerSample);
            return;
        }

        createAdminClient(routingContext.vertx(), acConfig).onComplete(ac -> {
            if (ac.failed()) {
                prom.fail(ac.cause());
            } else {
                ConsumerGroupOperations.resetGroupOffset(ac.result(), groupToReset, prom);
            }
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getResetGroupOffsetRequestTimer(), requestTimerSample);
        });
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
}
