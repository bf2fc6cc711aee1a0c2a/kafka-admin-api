package org.bf2.admin.kafka.admin.handlers;

import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.SaslConfigs;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.logging.Logger;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@RequestScoped
public class AdminClientFactory {

    protected static final String ADMIN_CLIENT_CONFIG = RestOperations.class.getName() + ".ADMIN_CLIENT_CONFIG";
    private static final String SASL_PLAIN_CONFIG_TEMPLATE = "org.apache.kafka.common.security.plain.PlainLoginModule "
            + "required "
            + "username=\"%s\" "
            + "password=\"%s\";";
    private static final String SASL_OAUTH_CONFIG_TEMPLATE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"%s\";";

    @Inject
    Logger log;

    @Inject
    Vertx vertx;

    @Inject
    KafkaAdminConfigRetriever config;

    @Inject
    Instance<JsonWebToken> token;

    @Inject
    HttpHeaders headers;

    /**
     * Route handler common to all Kafka resource routes. Responsible for creating
     * the map of properties used to configure the Kafka Admin Client. When OAuth
     * has been enabled via the environment, the access token will be retrieved from
     * the authenticated user principal present in the context (created by Vert.x
     * handler when a valid JWT was presented by the client). The configuration property
     * map will be placed in the context under the key identified by the
     * {@link #ADMIN_CLIENT_CONFIG} constant.
     */
    public AdminClient createAdminClient() {
        Map<String, Object> acConfig = config.getAcConfig();

        if (config.isOauthEnabled()) {
            if (token.isResolvable()) {
                final String accessToken = token.get().getRawToken();
                if (accessToken == null) {
                    throw new NotAuthorizedException(Response.status(Status.UNAUTHORIZED));
                }
                acConfig.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_OAUTH_CONFIG_TEMPLATE, accessToken));
            } else {
                log.warn("OAuth is enabled, but there is no JWT principal");
            }
        } else if (config.isBasicEnabled()) {
            extractCredentials(Optional.ofNullable(headers.getHeaderString(HttpHeaders.AUTHORIZATION)))
                .ifPresentOrElse(credentials -> acConfig.put(SaslConfigs.SASL_JAAS_CONFIG, credentials),
                    () -> {
                        throw new NotAuthorizedException("Invalid or missing credentials", Response.status(Status.UNAUTHORIZED).build());
                    });
        } else {
            log.debug("OAuth is disabled - no attempt to set access token in Admin Client config");
        }

        return AdminClient.create(acConfig);
    }

    Optional<String> extractCredentials(Optional<String> authorizationHeader) {
        return authorizationHeader
                .filter(Objects::nonNull)
                .filter(authn -> authn.startsWith("Basic "))
                .map(authn -> authn.substring("Basic ".length()))
                .map(Base64.getDecoder()::decode)
                .map(String::new)
                .filter(authn -> authn.indexOf(':') >= 0)
                .map(authn -> new String[] {
                    authn.substring(0, authn.indexOf(':')),
                    authn.substring(authn.indexOf(':') + 1)
                })
                .filter(credentials -> !credentials[0].isEmpty() && !credentials[1].isEmpty())
                .map(credentials -> String.format(SASL_PLAIN_CONFIG_TEMPLATE, credentials[0], credentials[1]));
    }
}
