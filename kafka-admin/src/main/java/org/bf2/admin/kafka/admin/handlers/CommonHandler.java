package org.bf2.admin.kafka.admin.handlers;

import io.vertx.ext.web.validation.BodyProcessorException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.bf2.admin.kafka.admin.InvalidConsumerGroupException;
import org.bf2.admin.kafka.admin.InvalidTopicException;
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
import io.vertx.ext.web.api.validation.ValidationException;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    protected static void setOAuthToken(Map acConfig, RoutingContext rc) {
        String token = rc.request().getHeader("Authorization");
        if (token != null) {
            if (token.startsWith("Bearer ")) {
                token = token.substring("Bearer ".length());
            }
            acConfig.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"" + token + "\";");
        }
    }

    protected static Future<KafkaAdminClient> createAdminClient(Vertx vertx, Map acConfig) {
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
                if (res.cause() instanceof UnknownTopicOrPartitionException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code());
                } else if (res.cause() instanceof GroupIdNotFoundException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code());
                } else if (res.cause() instanceof TimeoutException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                } else if (res.cause() instanceof GroupNotEmptyException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.LOCKED.code());
                } else if (res.cause() instanceof AuthenticationException ||
                    res.cause() instanceof AuthorizationException ||
                    res.cause() instanceof TokenExpiredException ||
                    (res.cause().getCause() instanceof SaslAuthenticationException
                            && res.cause().getCause().getMessage().contains("Authentication failed due to an invalid token"))) {
                    routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                } else if (res.cause() instanceof org.apache.kafka.common.errors.InvalidTopicException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof InvalidReplicationFactorException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof TopicExistsException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.CONFLICT.code());
                } else if (res.cause() instanceof InvalidRequestException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof InvalidConfigurationException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof IllegalArgumentException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof IllegalStateException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                } else if (res.cause() instanceof InvalidTopicException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof InvalidConsumerGroupException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof UnknownMemberIdException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof DecodeException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof ValidationException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof BodyProcessorException) {
                    routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                } else if (res.cause() instanceof KafkaException) {
                    // Most of the kafka related exceptions are extended from KafkaException
                    if (res.cause().getMessage().contains("Failed to find brokers to send")) {
                        routingContext.response().setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                    } else if (res.cause().getMessage().contains("JAAS configuration")) {
                        routingContext.response().setStatusCode(HttpResponseStatus.UNAUTHORIZED.code());
                    } else {
                        log.error("Unknown exception ", res.cause());
                        routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                    }
                } else {
                    log.error("Unknown exception ", res.cause());
                    routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                }

                JsonObject jo = new JsonObject();
                jo.put("code", routingContext.response().getStatusCode());
                if (routingContext.response().getStatusCode() == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
                    jo.put("error_message", HttpResponseStatus.INTERNAL_SERVER_ERROR.reasonPhrase());
                } else {
                    jo.put("error_message", res.cause().getMessage());
                    jo.put("class", res.cause().getClass().getSimpleName());
                }
                routingContext.response().end(jo.toBuffer());
                httpMetrics.getFailedRequestsCounter().increment();
                requestTimerSample.stop(timer);
                log.error("{} {}", res.cause().getClass(), res.cause().getMessage());
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
                    httpMetrics.getFailedRequestsCounter().increment();
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

    public static Predicate<String> byName(Pattern pattern, Promise prom) {
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
