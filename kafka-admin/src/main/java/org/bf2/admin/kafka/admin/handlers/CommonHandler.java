package org.bf2.admin.kafka.admin.handlers;

import io.strimzi.kafka.oauth.validator.TokenExpiredException;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.bf2.admin.kafka.admin.model.Types;
import org.jboss.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.Response.StatusType;

import java.security.GeneralSecurityException;
import java.util.Comparator;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class CommonHandler {

    static final Logger log = Logger.getLogger(CommonHandler.class);

    public static boolean isCausedBy(Throwable error, Class<? extends Throwable> searchCause) {
        Throwable cause = error;

        do {
            if (searchCause.isInstance(cause)) {
                return true;
            }
        } while (cause != cause.getCause() && (cause = cause.getCause()) != null);

        return false;
    }

    static ResponseBuilder errorResponse(Throwable error, StatusType status, String errorMessage) {
        final int statusCode = status.getStatusCode();
        ResponseBuilder response = Response.status(statusCode);
        Types.Error errorEntity = new Types.Error();

        errorEntity.setCode(statusCode);

        if (errorMessage != null) {
            errorEntity.setErrorMessage(errorMessage);
        } else if (status == Status.INTERNAL_SERVER_ERROR) {
            errorEntity.setErrorMessage(status.getReasonPhrase());
        } else {
            errorEntity.setErrorMessage(error.getMessage());
            errorEntity.setClassName(error.getClass().getSimpleName());
        }

        response.entity(errorEntity);

        return response;
    }

    @SuppressWarnings({ "checkstyle:CyclomaticComplexity" })
    static ResponseBuilder processFailure(Throwable failureCause) {
        StatusType status;
        String errorMessage = null;

        if (failureCause instanceof CompletionException) {
            failureCause = failureCause.getCause();
        }

        // TODO: Refactor this...
        if (failureCause instanceof WebApplicationException) {
            WebApplicationException cause = (WebApplicationException) failureCause;
            status = cause.getResponse().getStatusInfo();
        } else if (failureCause instanceof ExceptionInInitializerError) {
            failureCause = failureCause.getCause();
            status = Status.BAD_REQUEST;
        } else if (failureCause instanceof UnknownTopicOrPartitionException
                || failureCause instanceof GroupIdNotFoundException) {
            status = Status.NOT_FOUND;
        } else if (failureCause instanceof TimeoutException) {
            status = Status.SERVICE_UNAVAILABLE;
        } else if (failureCause instanceof SslAuthenticationException) {
            log.error("SSL exception", failureCause);
            status = Status.INTERNAL_SERVER_ERROR;
        } else if (failureCause instanceof GroupNotEmptyException) {
            // 423 Locked (WebDAV, RFC4918)
            status = new StatusType() {
                @Override
                public int getStatusCode() {
                    return 423;
                }

                @Override
                public Family getFamily() {
                    return Family.CLIENT_ERROR;
                }

                @Override
                public String getReasonPhrase() {
                    return "Locked";
                }
            };
        } else if (failureCause instanceof AuthorizationException) {
            status = Status.FORBIDDEN;
        } else if (failureCause instanceof AuthenticationException
            || failureCause instanceof TokenExpiredException
            || (failureCause.getCause() instanceof SaslAuthenticationException
                    && failureCause.getCause().getMessage().contains("Authentication failed due to an invalid token"))) {
            status = Status.UNAUTHORIZED;
        } else if (failureCause instanceof InvalidTopicException
                || failureCause instanceof PolicyViolationException
                || failureCause instanceof InvalidReplicationFactorException) {
            status = Status.BAD_REQUEST;
        } else if (failureCause instanceof TopicExistsException) {
            status = Status.CONFLICT;
        } else if (failureCause instanceof InvalidRequestException
                || failureCause instanceof InvalidConfigurationException
                || failureCause instanceof IllegalArgumentException
                || failureCause instanceof InvalidPartitionsException) {
            status = Status.BAD_REQUEST;
        } else if (failureCause instanceof com.fasterxml.jackson.core.JsonParseException
                || failureCause instanceof com.fasterxml.jackson.databind.JsonMappingException) {
            status = Status.BAD_REQUEST;
            errorMessage = "invalid JSON";
        } else if (failureCause instanceof IllegalStateException) {
            status = Status.UNAUTHORIZED;
        } else if (failureCause instanceof DecodeException
                || failureCause instanceof UnknownMemberIdException
                || failureCause instanceof LeaderNotAvailableException) {
            status = Status.BAD_REQUEST;
        } else if (failureCause instanceof KafkaException) {
            // Most of the kafka related exceptions are extended from KafkaException
            if (failureCause.getMessage().contains("Failed to find brokers to send")) {
                status = Status.SERVICE_UNAVAILABLE;
            } else if (failureCause.getMessage().contains("JAAS configuration")) {
                status = Status.UNAUTHORIZED;
            } else {
                log.error("Unknown exception", failureCause);
                status = Status.INTERNAL_SERVER_ERROR;
            }
        } else if (failureCause instanceof RuntimeException) {
            RuntimeException iae = (RuntimeException) failureCause;
            if (iae.getCause() instanceof GeneralSecurityException) {
                failureCause = iae.getCause();
                status = Status.UNAUTHORIZED;
            } else {
                log.error("Unknown exception", iae.getCause());
                status = Status.INTERNAL_SERVER_ERROR;
            }
        } else {
            log.error("Unknown exception", failureCause);
            status = Status.INTERNAL_SERVER_ERROR;
        }

        if (status.getFamily() == Family.SERVER_ERROR) {
            log.errorf(failureCause, "%s %s", failureCause.getClass(), failureCause.getMessage());
        } else {
            log.warnf("%s %s", failureCause.getClass(), failureCause.getMessage());
        }

        return errorResponse(failureCause, status, errorMessage);
    }

    public static class TopicComparator implements Comparator<Types.Topic> {

        private final String key;
        public TopicComparator(String key) {
            this.key = key;
        }

        public TopicComparator() {
            this.key = "name";
        }
        @Override
        public int compare(Types.Topic firstTopic, Types.Topic secondTopic) {

            if ("name".equals(key)) {
                return firstTopic.getName().compareToIgnoreCase(secondTopic.getName());
            } else if ("partitions".equals(key)) {
                return firstTopic.getPartitions().size() - secondTopic.getPartitions().size();
            } else if ("retention.ms".equals(key)) {
                Types.ConfigEntry first = firstTopic.getConfig().stream().filter(entry -> entry.getKey().equals("retention.ms")).findFirst().orElseGet(() -> null);
                Types.ConfigEntry second = secondTopic.getConfig().stream().filter(entry -> entry.getKey().equals("retention.ms")).findFirst().orElseGet(() -> null);
                if (first == null || second == null || first.getValue() == null || second.getValue() == null) {
                    return 0;
                } else {
                    return Long.compare(first.getValue().equals("-1") ? Long.MAX_VALUE : Long.parseLong(first.getValue()), second.getValue().equals("-1") ? Long.MAX_VALUE : Long.parseLong(second.getValue()));
                }
            } else if ("retention.bytes".equals(key)) {
                Types.ConfigEntry first = firstTopic.getConfig().stream().filter(entry -> entry.getKey().equals("retention.bytes")).findFirst().orElseGet(() -> null);
                Types.ConfigEntry second = secondTopic.getConfig().stream().filter(entry -> entry.getKey().equals("retention.bytes")).findFirst().orElseGet(() -> null);
                if (first == null || second == null || first.getValue() == null || second.getValue() == null) {
                    return 0;
                } else {
                    return Long.compare(first.getValue().equals("-1") ? Long.MAX_VALUE : Long.parseLong(first.getValue()), second.getValue().equals("-1") ? Long.MAX_VALUE : Long.parseLong(second.getValue()));
                }
            }
            return 0;
        }
    }

    public static class ConsumerGroupComparator implements Comparator<Types.ConsumerGroup> {

        private final String key;
        public ConsumerGroupComparator(String key) {
            this.key = key;
        }

        public ConsumerGroupComparator() {
            this.key = "name";
        }

        @Override
        public int compare(Types.ConsumerGroup firstConsumerGroup, Types.ConsumerGroup secondConsumerGroup) {
            if ("name".equals(key)) {
                if (firstConsumerGroup == null || firstConsumerGroup.getGroupId() == null
                    || secondConsumerGroup == null || secondConsumerGroup.getGroupId() == null) {
                    return 0;
                } else {
                    return firstConsumerGroup.getGroupId().compareToIgnoreCase(secondConsumerGroup.getGroupId());
                }
            }
            return 0;
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
