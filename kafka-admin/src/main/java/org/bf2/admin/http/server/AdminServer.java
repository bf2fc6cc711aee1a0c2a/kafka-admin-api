package org.bf2.admin.http.server;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.pointer.JsonPointer;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2FlowType;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.HSTSHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import io.vertx.ext.web.openapi.OpenAPIHolder;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import io.vertx.ext.web.openapi.impl.ContractEndpointHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.Operations;
import org.bf2.admin.kafka.admin.handlers.RestOperations;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * The main Kafka Admin API Server class. It is a Vert.x {@link io.vertx.core.Verticle} and it starts
 * an HTTP server which listen for inbound HTTP requests.
 * <p>
 * The routes are split into two groups, management and resources. Management routes include metrics and
 * health checks and are exposed in clear text on port 9990. Resource routes include the Kafka entities
 * accessed by users of the admin server and are exposed using TLS on port 8443 or in clear text on port
 * 8080 if no certificate is provided.
 */
@ApplicationScoped
public class AdminServer {

    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);
    private static final String REST_API_SPEC = "META-INF/openapi.yaml";
    private static final String SECURITY_SCHEME_NAME_OAUTH = "Bearer";
    private static final String SECURITY_SCHEME_NAME_BASIC = "BasicAuth";
    private static final Decoder BASE64_DECODER = Base64.getDecoder();

    @Inject
    Vertx vertx;

    @Inject
    HttpMetrics httpMetrics;

    @Inject
    KafkaAdminConfigRetriever config;

    public void init(@Observes Router router) {
        try {
            router.route()
                .handler(HSTSHandler.create(Duration.ofDays(365).toSeconds(), false));

            RouterBuilder.create(vertx, REST_API_SPEC)
                .compose(builder -> buildResourcesRouter(router, builder))
                .toCompletionStage()
                .toCompletableFuture()
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<Void> buildResourcesRouter(Router router, RouterBuilder builder) {
        final Future<Void> result;
        final YAMLMapper yamlMapper = new YAMLMapper();
        final JsonObject openAPI = builder.getOpenAPI().getOpenAPI();
        JsonObject hostedOpenAPI;

        /*
         * Read the OpenAPI document separately for hosting. Work-around for
         * https://github.com/vert-x3/vertx-web/issues/1996
         */
        try (InputStream stream = getClass().getResourceAsStream('/' + REST_API_SPEC)) {
            hostedOpenAPI = JsonObject.mapFrom(yamlMapper.readTree(stream));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        //final RouterBuilderOptions options = new RouterBuilderOptions();

        // OpenAPI contract document served at `/rest/openapi`
        //options.setContractEndpoint(RouterBuilderOptions.STANDARD_CONTRACT_ENDPOINT);
        //builder.setOptions(options);

        updateOpenAPISecurity(openAPI);
        updateOpenAPISecurity(hostedOpenAPI);

        if (config.isOauthEnabled()) {
            result = configureOAuth(builder);
        } else if (config.isBasicEnabled()) {
            result = configureHttpBasicAuth(builder);
        } else {
            result = Future.succeededFuture();
        }

        assignRoutes(builder);

        Router restRouter = builder.createRouter();

        // OpenAPI contract document served at `/rest/openapi`
        restRouter.get(RouterBuilderOptions.STANDARD_CONTRACT_ENDPOINT)
            .handler(ContractEndpointHandler.create(new OpenAPIHolder() {
                @Override
                public JsonObject solveIfNeeded(JsonObject obj) {
                    return null;
                }

                @Override
                public JsonObject getOpenAPI() {
                    return hostedOpenAPI;
                }

                @Override
                public JsonObject getCached(JsonPointer pointer) {
                    return null;
                }
            }));

        router.mountSubRouter("/rest", restRouter);
        return result;
    }

    private void updateOpenAPISecurity(JsonObject openAPI) {
        JsonObject securitySchemas = openAPI.getJsonObject("components")
                .getJsonObject("securitySchemes");

        if (config.isOauthEnabled()) {
            JsonObject clientCredentialsFlow = securitySchemas
                    .getJsonObject(SECURITY_SCHEME_NAME_OAUTH)
                    .getJsonObject("flows")
                    .getJsonObject("clientCredentials");

            if (config.getOauthTokenEndpointUri() != null) {
                LOGGER.info("Setting Oauth2 token endpoint URL: {}", config.getOauthTokenEndpointUri());
                clientCredentialsFlow.put("tokenUrl", config.getOauthTokenEndpointUri());
            } else {
                clientCredentialsFlow.remove("tokenUrl");
            }
        } else if (config.isBasicEnabled()) {
            JsonArray security = openAPI.getJsonArray("security");
            Iterator<Object> items = security.iterator();

            while (items.hasNext()) {
                Object item = items.next();
                if (item instanceof JsonObject && ((JsonObject) item).containsKey(SECURITY_SCHEME_NAME_OAUTH)) {
                    items.remove();
                }
            }

            security.add(new JsonObject().put(SECURITY_SCHEME_NAME_BASIC, new JsonArray()));
            securitySchemas.remove(SECURITY_SCHEME_NAME_OAUTH);
            securitySchemas.put(SECURITY_SCHEME_NAME_BASIC, new JsonObject()
                                .put("type", "http")
                                .put("scheme", "basic"));
        } else {
            openAPI.remove("security");
            openAPI.getJsonObject("components").remove("securitySchemes");
        }
    }

    private Future<Void> configureOAuth(RouterBuilder builder) {
        if (config.getOauthJwksEndpointUri() == null) {
            return Future.failedFuture(String.format(""
                    + "Environment variable `%s` must be provided when OAuth is enabled (`%s` is unset or `true`)",
                    KafkaAdminConfigRetriever.OAUTH_JWKS_ENDPOINT_URI,
                    KafkaAdminConfigRetriever.OAUTH_ENABLED));
        }

        OAuth2Options oauthOptions = new OAuth2Options();
        oauthOptions.setFlow(OAuth2FlowType.CLIENT);
        oauthOptions.setJwkPath(config.getOauthJwksEndpointUri());
        oauthOptions.setValidateIssuer(true);

        String oauthTrustedCertificate = config.getOauthTrustedCertificate();

        if (oauthTrustedCertificate != null) {
            PemTrustOptions trustOptions = new PemTrustOptions();
            setCertConfig(oauthTrustedCertificate, trustOptions::addCertPath, trustOptions::addCertValue);
            oauthOptions.setHttpClientOptions(new HttpClientOptions().setPemTrustOptions(trustOptions));
        }

        if (config.getOauthValidIssuerUri() != null) {
            LOGGER.info("JWT issuer (iss) claim valid value: {}", config.getOauthValidIssuerUri());
            oauthOptions.setJWTOptions(new JWTOptions().setIssuer(config.getOauthValidIssuerUri()));
        }

        OAuth2Auth oauth2Provider = OAuth2Auth.create(vertx, oauthOptions);
        OAuth2AuthHandler securityHandler = OAuth2AuthHandler.create(vertx, oauth2Provider);
        builder.securityHandler(SECURITY_SCHEME_NAME_OAUTH, securityHandler);

        return oauth2Provider.jWKSet()
                .onSuccess(ignored -> LOGGER.info("Loaded JWKS from {}", config.getOauthJwksEndpointUri()))
                .onFailure(cause -> LOGGER.error("Failed to retrieve JWKS: {}", cause.getMessage()));
    }

    private Future<Void> configureHttpBasicAuth(RouterBuilder builder) {
        builder.securityHandler(SECURITY_SCHEME_NAME_BASIC,
                                BasicAuthHandler.create(this::httpBasicAuthProvider, "kafka-admin-server"));

        return Future.succeededFuture();
    }

    private void httpBasicAuthProvider(JsonObject credentials, Handler<AsyncResult<User>> handler) {
        String username = Objects.requireNonNullElse(credentials.getString("username"), "");
        String password = Objects.requireNonNullElse(credentials.getString("password"), "");

        if (username.isEmpty() || password.isEmpty()) {
            handler.handle(Future.failedFuture("Invalid or missing credentials"));
        } else {
            handler.handle(Future.succeededFuture(User.create(credentials)));
        }
    }

    private void assignRoutes(final RouterBuilder routerFactory) {
        RestOperations ro = new RestOperations(config, httpMetrics);

        Map<String, Handler<RoutingContext>> routes = Map.ofEntries(Map.entry(Operations.GET_TOPIC, ro::describeTopic),
                                                             Map.entry(Operations.GET_TOPICS_LIST, ro::listTopics),
                                                             Map.entry(Operations.DELETE_TOPIC, ro::deleteTopic),
                                                             Map.entry(Operations.CREATE_TOPIC, ro::createTopic),
                                                             Map.entry(Operations.UPDATE_TOPIC, ro::updateTopic),
                                                             Map.entry(Operations.GET_CONSUMER_GROUP, ro::describeGroup),
                                                             Map.entry(Operations.GET_CONSUMER_GROUPS_LIST, ro::listGroups),
                                                             Map.entry(Operations.DELETE_CONSUMER_GROUP, ro::deleteGroup),
                                                             Map.entry(Operations.RESET_CONSUMER_GROUP_OFFSET, ro::resetGroupOffset),
                                                             Map.entry(Operations.GET_ACL_RESOURCE_OPERATIONS, ro::getAclResourceOperations),
                                                             Map.entry(Operations.GET_ACLS, ro::describeAcls),
                                                             Map.entry(Operations.CREATE_ACL, ro::createAcl),
                                                             Map.entry(Operations.DELETE_ACLS, ro::deleteAcls));

        routes.entrySet().forEach(route ->
            routerFactory.operation(route.getKey())
                .handler(context -> {
                    // Setup AdminClient configuration for all routes before invoking handler
                    ro.setAdminClientConfig(context);
                    route.getValue().handle(context);
                })
                // Common error handling for all routes
                .failureHandler(ro::errorHandler));
    }

    private void setCertConfig(String value, Consumer<String> pathSetter, Consumer<Buffer> valueSetter) {
        boolean readableFile;

        try {
            readableFile = Files.isReadable(Path.of(value));
        } catch (Exception e) {
            LOGGER.info("Exception checking if cert config value is a file: {}", e.getMessage());
            readableFile = false;
        }

        if (readableFile) {
            pathSetter.accept(value);
        } else {
            try {
                value = new String(BASE64_DECODER.decode(value), StandardCharsets.UTF_8);
                LOGGER.debug("Successfully decoded base-64 cert config value");
            } catch (IllegalArgumentException e) {
                LOGGER.debug("Cert config value was not base-64 encoded: {}", e.getMessage());
            }

            valueSetter.accept(Buffer.buffer(value));
        }
    }
}
