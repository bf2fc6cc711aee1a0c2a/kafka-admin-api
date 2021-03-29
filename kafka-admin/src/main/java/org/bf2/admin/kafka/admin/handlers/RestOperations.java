package org.bf2.admin.kafka.admin.handlers;

import org.bf2.admin.kafka.admin.ConsumerGroupOperations;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.TopicOperations;
import org.bf2.admin.kafka.admin.model.Types;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.bf2.admin.kafka.admin.HttpMetrics;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.errors.InvalidRequestException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RestOperations extends CommonHandler implements OperationsHandler<Handler<RoutingContext>> {
    @Override
    public Handler<RoutingContext> createTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getCreateTopicCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            createAdminClient(vertx, acConfig).onComplete(ac -> {
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
                    httpMetrics.getFailedRequestsCounter().increment();
                    requestTimerSample.stop(httpMetrics.getCreateTopicRequestTimer());
                    return;
                }

                if (!internalTopicsAllowed() && inputTopic.getName().startsWith("__")) {
                    prom.fail(new InvalidTopicException("Topic " + inputTopic.getName() + " cannot be created"));
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
        };
    }

    @Override
    public Handler<RoutingContext> describeTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getDescribeTopicCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            String uri = routingContext.request().uri();
            String topicToDescribe = uri.substring(uri.lastIndexOf("/") + 1);
            Promise<Types.Topic> prom = Promise.promise();
            if (topicToDescribe == null || topicToDescribe.isEmpty()) {
                prom.fail(new InvalidTopicException("Topic to describe has not been specified."));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
            }
            if (!internalTopicsAllowed() && topicToDescribe.startsWith("__")) {
                prom.fail(new InvalidTopicException("Topic " + topicToDescribe + " cannot be described"));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
                return;
            }

            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    TopicOperations.describeTopic(ac.result(), prom, topicToDescribe);
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDescribeTopicRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> updateTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getUpdateTopicCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            Promise<Types.UpdatedTopic> prom = Promise.promise();
            String uri = routingContext.request().uri();
            String topicToUpdate = uri.substring(uri.lastIndexOf("/") + 1);
            if (topicToUpdate == null || topicToUpdate.isEmpty()) {
                prom.fail(new InvalidTopicException("Topic to update has not been specified."));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
            }

            if (!internalTopicsAllowed() && topicToUpdate.startsWith("__")) {
                prom.fail(new InvalidTopicException("Topic " + topicToUpdate + " cannot be updated"));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
                return;
            }

            createAdminClient(vertx, acConfig).onComplete(ac -> {
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
                        httpMetrics.getFailedRequestsCounter().increment();
                        prom.fail(e);
                        log.error(e);
                        return;
                    }
                    TopicOperations.updateTopic(ac.result(), updatedTopic, prom);
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getUpdateTopicRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> deleteTopic(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getDeleteTopicCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            String uri = routingContext.request().uri();
            String topicToDelete = uri.substring(uri.lastIndexOf("/") + 1);
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

            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    TopicOperations.deleteTopics(ac.result(), Collections.singletonList(topicToDelete), prom);
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDeleteTopicRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> listTopics(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            httpMetrics.getListTopicsCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            setOAuthToken(acConfig, routingContext);
            String filter = routingContext.queryParams().get("filter");
            String limit = routingContext.queryParams().get("limit") == null ? "0" : routingContext.queryParams().get("limit");
            String offset = routingContext.queryParams().get("offset") == null ? "0" : routingContext.queryParams().get("offset");
            Types.SortDirectionEnum sortReverse = routingContext.queryParams().get("order") == null ?
                Types.SortDirectionEnum.ASC : "desc".equals(routingContext.queryParams().get("order")) ? Types.SortDirectionEnum.DESC : Types.SortDirectionEnum.ASC;
            final Pattern pattern;
            Promise<Types.TopicList> prom = Promise.promise();
            if (filter != null && !filter.isEmpty()) {
                pattern = Pattern.compile(filter);
            } else {
                pattern = null;
            }

            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    try {
                        if (Integer.parseInt(offset) < 0 || Integer.parseInt(limit) < 0) {
                            throw new InvalidRequestException("Offset and limit have to be positive integers.");
                        }
                        TopicOperations.getTopicList(ac.result(), prom, pattern, Integer.parseInt(offset), Integer.parseInt(limit), sortReverse);
                    } catch (NumberFormatException | InvalidRequestException e) {
                        prom.fail(e);
                        processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getListTopicRequestTimer(), requestTimerSample);
                    }
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getListTopicRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> listGroups(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            httpMetrics.getListGroupsCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            setOAuthToken(acConfig, routingContext);
            String filter = routingContext.queryParams().get("filter");
            String limit = routingContext.queryParams().get("limit") == null ? "0" : routingContext.queryParams().get("limit");
            String offset = routingContext.queryParams().get("offset") == null ? "0" : routingContext.queryParams().get("offset");
            final Pattern pattern;
            Promise<Types.TopicList> prom = Promise.promise();
            if (filter != null && !filter.isEmpty()) {
                pattern = Pattern.compile(filter);
            } else {
                pattern = null;
            }

            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    try {
                        if (Integer.parseInt(offset) < 0 || Integer.parseInt(limit) < 0) {
                            throw new InvalidRequestException("Offset and limit have to be positive integers.");
                        }
                        ConsumerGroupOperations.getGroupList(ac.result(), prom, pattern, Integer.parseInt(offset), Integer.parseInt(limit));
                    } catch (NumberFormatException | InvalidRequestException e) {
                        prom.fail(e);
                        processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getListGroupsRequestTimer(), requestTimerSample);
                    }
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getListGroupsRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> describeGroup(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getDescribeGroupCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            String uri = routingContext.request().uri();
            String groupToDescribe = uri.substring(uri.lastIndexOf("/") + 1);
            Promise<Types.Topic> prom = Promise.promise();
            if (groupToDescribe == null || groupToDescribe.isEmpty()) {
                prom.fail(new InvalidConsumerGroupException("Consumer ConsumerGroup to describe has not been specified."));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDescribeGroupRequestTimer(), requestTimerSample);
            }
            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    ConsumerGroupOperations.describeGroup(ac.result(), prom, Collections.singletonList(groupToDescribe));
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDescribeGroupRequestTimer(), requestTimerSample);
            });
        };
    }

    @Override
    public Handler<RoutingContext> deleteGroup(KafkaAdminConfigRetriever kaConfig, Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            Map<String, Object> acConfig = kaConfig.getAcConfig();
            httpMetrics.getDeleteGroupCounter().increment();
            httpMetrics.getRequestsCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            setOAuthToken(acConfig, routingContext);
            String uri = routingContext.request().uri();
            String groupToDelete = uri.substring(uri.lastIndexOf("/") + 1);
            Promise<List<String>> prom = Promise.promise();
            if (groupToDelete == null || groupToDelete.isEmpty()) {
                prom.fail(new InvalidConsumerGroupException("Consumer ConsumerGroup to delete has not been specified."));
                processResponse(prom, routingContext, HttpResponseStatus.BAD_REQUEST, httpMetrics, httpMetrics.getDeleteGroupRequestTimer(), requestTimerSample);
                return;
            }

            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    ConsumerGroupOperations.deleteGroup(ac.result(), Collections.singletonList(groupToDelete), prom);
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getDeleteGroupRequestTimer(), requestTimerSample);
            });
        };
    }

    public Handler<RoutingContext> openApi(Vertx vertx, HttpMetrics httpMetrics) {
        return routingContext -> {
            httpMetrics.getRequestsCounter().increment();
            httpMetrics.getOpenApiCounter().increment();
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());

            Promise<List<String>> prom = Promise.promise();

            FileSystem fileSystem = vertx.fileSystem();

            String filename = "./rest/src/main/resources/openapi-specs/rest.yaml";
            fileSystem.readFile(filename, readFile -> {
                if (readFile.succeeded()) {
                    routingContext.response().sendFile(filename);
                } else {
                    log.error("Failed to read OpenAPI YAML file", readFile.cause());
                    prom.fail(readFile.cause());
                }
                processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getOpenApiRequestTimer(), requestTimerSample);
            });
        };
    }

    private boolean internalTopicsAllowed() {
        return System.getenv("INTERNAL_TOPICS_ENABLED") == null ? false : Boolean.valueOf(System.getenv("INTERNAL_TOPICS_ENABLED"));
    }

    public Handler<RoutingContext> errorHandler(HttpMetrics httpMetrics) {
        return routingContext -> {
            Timer.Sample requestTimerSample = Timer.start(httpMetrics.getRegistry());
            Promise<List<String>> prom = Promise.promise();
            prom.fail(routingContext.failure());
            processResponse(prom, routingContext, HttpResponseStatus.OK, httpMetrics, httpMetrics.getOpenApiRequestTimer(), requestTimerSample);
        };
    }
}
