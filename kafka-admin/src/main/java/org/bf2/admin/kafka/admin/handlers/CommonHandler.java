package org.bf2.admin.kafka.admin.handlers;

import io.strimzi.kafka.oauth.validator.TokenExpiredException;
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
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class CommonHandler {

    static final Logger log = Logger.getLogger(CommonHandler.class);

    private CommonHandler() {
    }

    /**
     * 423 Locked (WebDAV, RFC4918)
     */
    static final StatusType LOCKED = new StatusType() {
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

    static Map<Class<? extends Throwable>, Function<? extends Throwable, ResponseBuilder>> errorHandlers = Map.ofEntries(
            entry(WebApplicationException.class, thrown -> errorResponse(thrown, thrown.getResponse().getStatusInfo(), null)),
            entry(ExceptionInInitializerError.class, thrown -> errorResponse(thrown.getCause(), Status.BAD_REQUEST, null)),
            entry(UnknownTopicOrPartitionException.class, thrown -> errorResponse(thrown, Status.NOT_FOUND, null)),
            entry(GroupIdNotFoundException.class, thrown -> errorResponse(thrown, Status.NOT_FOUND, null)),
            entry(TimeoutException.class, thrown -> errorResponse(thrown, Status.SERVICE_UNAVAILABLE, null)),
            entry(SslAuthenticationException.class, thrown -> errorResponse(thrown, Status.INTERNAL_SERVER_ERROR, null)),
            entry(GroupNotEmptyException.class, thrown -> errorResponse(thrown, LOCKED, null)),
            entry(AuthorizationException.class, thrown -> errorResponse(thrown, Status.FORBIDDEN, null)),
            entry(AuthenticationException.class, thrown -> errorResponse(thrown, Status.UNAUTHORIZED, null)),
            entry(TokenExpiredException.class, thrown -> errorResponse(thrown, Status.UNAUTHORIZED, null)),
            entry(InvalidTopicException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(PolicyViolationException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(InvalidReplicationFactorException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(TopicExistsException.class, thrown -> errorResponse(thrown, Status.CONFLICT, null)),
            entry(InvalidRequestException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(InvalidConfigurationException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(IllegalArgumentException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(InvalidPartitionsException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(com.fasterxml.jackson.core.JsonParseException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, "invalid JSON")),
            entry(com.fasterxml.jackson.databind.JsonMappingException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, "invalid JSON")),
            entry(IllegalStateException.class, thrown -> errorResponse(thrown, Status.UNAUTHORIZED, null)),
            entry(DecodeException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(UnknownMemberIdException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(LeaderNotAvailableException.class, thrown -> errorResponse(thrown, Status.BAD_REQUEST, null)),
            entry(KafkaException.class, thrown -> {
                    if (thrown.getMessage().contains("Failed to find brokers to send")) {
                        return errorResponse(thrown, Status.SERVICE_UNAVAILABLE, null);
                    } else if (thrown.getMessage().contains("JAAS configuration")) {
                        return errorResponse(thrown, Status.UNAUTHORIZED, null);
                    } else {
                        log.error("Unknown exception", thrown);
                        return errorResponse(thrown, Status.INTERNAL_SERVER_ERROR, null);
                    }
                }),
            entry(GeneralSecurityException.class, thrown -> errorResponse(thrown, Status.UNAUTHORIZED, null)));

    static <T extends Throwable> Map.Entry<Class<T>, Function<T, ResponseBuilder>> entry(Class<T> key, Function<T, ResponseBuilder> value) {
        return Map.entry(key, value);
    }

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

        if (status.getFamily() == Family.SERVER_ERROR) {
            log.errorf(error, "%s %s", error.getClass(), error.getMessage());
        } else {
            log.warnf("%s %s", error.getClass(), error.getMessage());
        }

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

    @SuppressWarnings("unchecked")
    public static ResponseBuilder mapCause(Throwable error, Class<? extends Throwable> searchCause, Function<? extends Throwable, ResponseBuilder> mapper) {
        Throwable cause = error;

        do {
            if (searchCause.isInstance(cause)) {
                return ((Function<Throwable, ResponseBuilder>) mapper).apply(cause);
            }
        } while (cause != cause.getCause() && (cause = cause.getCause()) != null);

        return null;
    }

    static ResponseBuilder processFailure(Throwable thrown) {
        return errorHandlers.entrySet()
            .stream()
            .map(e -> mapCause(thrown, e.getKey(), e.getValue()))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(() -> {
                log.error("Unknown exception", thrown);
                return errorResponse(thrown, Status.INTERNAL_SERVER_ERROR, null);
            });
    }

}
