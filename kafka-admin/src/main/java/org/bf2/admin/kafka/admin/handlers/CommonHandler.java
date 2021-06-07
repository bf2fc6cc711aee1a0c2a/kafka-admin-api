package org.bf2.admin.kafka.admin.handlers;

import io.vertx.ext.web.validation.BodyProcessorException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.model.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micrometer.core.instrument.Timer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.oauth.validator.TokenExpiredException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.json.schema.ValidationException;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.model.Types;

import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@SuppressWarnings({"checkstyle:CyclomaticComplexity"})
public class CommonHandler {

    protected static final Logger log = LogManager.getLogger(CommonHandler.class);
    protected static final String ADMIN_CLIENT_CONFIG = RestOperations.class.getName() + ".ADMIN_CLIENT_CONFIG";
    private static final String SASL_CONFIG_TEMPLATE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"%s\";";

    protected final KafkaAdminConfigRetriever kaConfig;

    protected CommonHandler(KafkaAdminConfigRetriever config) {
        this.kaConfig = config;
    }

    /**
     * Route handler common to all Kafka resource routes. Responsible for creating
     * the map of properties used to configure the Kafka Admin Client. When OAuth
     * has been enabled via the environment, the access token will be retrieved from
     * the authenticated user principal present in the context (created by Vert.x
     * handler when a valid JWT was presented by the client). The configuration property
     * map will be placed in the context under the key identified by the
     * {@link #ADMIN_CLIENT_CONFIG} constant.
     *
     * @param context
     */
    public void setAdminClientConfig(RoutingContext context) {
        Map<String, Object> acConfig = kaConfig.getAcConfig();

        if (kaConfig.isOauthEnabled()) {
            final String accessToken = context.user().principal().getString("access_token");
            acConfig.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_CONFIG_TEMPLATE, accessToken));
        } else {
            log.debug("OAuth is disabled - no attempt to set access token in Admin Client config");
        }

        context.put(ADMIN_CLIENT_CONFIG, acConfig);
    }

    protected static Future<KafkaAdminClient> createAdminClient(Vertx vertx, Map<String, Object> acConfig) {
        Properties props = new Properties();
        props.putAll(acConfig);

        KafkaAdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(vertx, props);
            return Future.succeededFuture(adminClient);
        } catch (Exception e) {
            log.error("Failed to create Kafka AdminClient", e.getCause());
            if (adminClient != null) {
                adminClient.close();
            }
            return Future.failedFuture(new KafkaException(e.getCause().getMessage()));
        }
    }

    protected static <T> void processResponse(Promise<T> prom, RoutingContext routingContext, HttpResponseStatus successResponseStatus, HttpMetrics httpMetrics, Timer timer, Timer.Sample requestTimerSample) {
        prom.future().onComplete(res -> {
            if (res.failed()) {
                Throwable failureCause = res.cause();

                // TODO: Refactor this...
                if (failureCause instanceof HttpStatusException) {
                    HttpStatusException cause = (HttpStatusException) failureCause;
                    routingContext.response().setStatusCode(cause.getStatusCode());
                } else if (failureCause instanceof UnknownTopicOrPartitionException
                        || failureCause instanceof GroupIdNotFoundException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code());
                } else if (failureCause instanceof TimeoutException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                } else if (failureCause instanceof GroupNotEmptyException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.LOCKED.code());
                } else if (failureCause instanceof GroupAuthorizationException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.FORBIDDEN.code());
                } else if (failureCause instanceof AuthenticationException
                    || failureCause instanceof AuthorizationException
                    || failureCause instanceof TokenExpiredException
                    || (failureCause.getCause() instanceof SaslAuthenticationException
                            && failureCause.getCause().getMessage().contains("Authentication failed due to an invalid token"))) {
                    routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                } else if (failureCause instanceof org.apache.kafka.common.errors.InvalidTopicException
                        || failureCause instanceof InvalidReplicationFactorException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (failureCause instanceof TopicExistsException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.CONFLICT.code());
                } else if (failureCause instanceof InvalidRequestException
                        || failureCause instanceof InvalidConfigurationException
                        || failureCause instanceof IllegalArgumentException
                        || res.cause() instanceof InvalidReplicationFactorException
                        || res.cause() instanceof org.apache.kafka.common.errors.InvalidTopicException
                        || res.cause() instanceof InvalidPartitionsException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (failureCause instanceof IllegalStateException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                } else if (failureCause instanceof DecodeException
                        || failureCause instanceof ValidationException
                        || failureCause instanceof InvalidTopicException
                        || failureCause instanceof BodyProcessorException
                        || failureCause instanceof UnknownMemberIdException
                        || failureCause instanceof InvalidConsumerGroupException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (failureCause instanceof KafkaException) {
                    // Most of the kafka related exceptions are extended from KafkaException
                    if (failureCause.getMessage().contains("Failed to find brokers to send")) {
                        routingContext.response().setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                    } else if (failureCause.getMessage().contains("JAAS configuration")) {
                        routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                    } else {
                        log.error("Unknown exception ", failureCause);
                        routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    }
                } else if (failureCause instanceof RuntimeException) {
                    RuntimeException iae = (RuntimeException) failureCause;
                    if (iae.getCause() instanceof GeneralSecurityException) {
                        failureCause = iae.getCause();
                        routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                    } else {
                        log.error("Unknown exception ", iae.getCause());
                        routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    }
                } else {
                    log.error("Unknown exception ", failureCause);
                    routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                }

                JsonObject jo = new JsonObject();
                jo.put("code", routingContext.response().getStatusCode());
                if (routingContext.response().getStatusCode() == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
                    jo.put("error_message", HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
                } else {
                    jo.put("error_message", failureCause.getMessage());
                    jo.put("class", failureCause.getClass().getSimpleName());
                }
                routingContext.response().end(jo.toBuffer());
                httpMetrics.getFailedRequestsCounter(routingContext.response().getStatusCode()).increment();
                requestTimerSample.stop(timer);
                log.error("{} {}", failureCause.getClass(), failureCause.getMessage());
            } else {
                ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                String json = null;
                try {
                    json = ow.writeValueAsString(res.result());
                } catch (JsonProcessingException e) {
                    routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.put("code", routingContext.response().getStatusCode());
                    jsonObject.put("error", e.getMessage());
                    routingContext.response().end(jsonObject.toBuffer());
                    httpMetrics.getFailedRequestsCounter(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).increment();
                    requestTimerSample.stop(timer);
                    log.error(e);
                    return;
                }
                routingContext.response().setStatusCode(successResponseStatus.code());
                routingContext.response().end(json);
                httpMetrics.getSucceededRequestsCounter().increment();
                requestTimerSample.stop(timer);
            }
        });
    }

    public static class TopicComparator implements Comparator<Types.Topic> {
        @Override
        public int compare(Types.Topic firstTopic, Types.Topic secondTopic) {
            return firstTopic.getName().compareTo(secondTopic.getName());
        }
    }

    public static class ConsumerGroupComparator implements Comparator<Types.ConsumerGroup> {
        @Override
        public int compare(Types.ConsumerGroup firstConsumerGroup, Types.ConsumerGroup secondConsumerGroup) {
            return firstConsumerGroup.getGroupId().compareTo(secondConsumerGroup.getGroupId());
        }
    }

    public static Predicate<String> byName(Pattern pattern, Promise<?> prom) {
        return topic -> {
            if (pattern == null) {
                return true;
            } else {
                try {
                    Matcher matcher = pattern.matcher(topic);
                    return matcher.find();
                } catch (PatternSyntaxException ex) {
                    prom.fail(ex);
                    return false;
                }
            }
        };
    }
}
