package org.bf2.admin.http.server;


import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.OAuth2FlowType;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.HSTSHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.Operations;
import org.bf2.admin.kafka.admin.handlers.RestOperations;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Map;
import java.util.Set;
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
public class AdminServer extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);
    private static final int HTTP_PORT = 8080;
    private static final int HTTPS_PORT = 8443;
    private static final int MANAGEMENT_PORT = 9990;
    private static final String SUCCESS_RESPONSE = "{\"status\": \"OK\"}";
    private static final String SECURITY_SCHEME_NAME = "Bearer";
    private static final Decoder BASE64_DECODER = Base64.getDecoder();

    private final KafkaAdminConfigRetriever config = new KafkaAdminConfigRetriever();
    private final HttpMetrics httpMetrics = new HttpMetrics();

    @Override
    public void start(final Promise<Void> startServer) {
        startManagementServer()
            .compose(nothing -> startResourcesServer())
            .onFailure(startServer::fail);
    }

    private Future<Void> startManagementServer() {
        Promise<Void> promise = Promise.promise();

        addHealthRouter(Router.router(vertx))
                .compose(this::addMetricsRouter)
                .compose(router -> vertx.createHttpServer().requestHandler(router).listen(MANAGEMENT_PORT))
                .onSuccess(server -> {
                    LOGGER.info("Admin Server management is listening on port {}", MANAGEMENT_PORT);
                    promise.complete();
                })
                .onFailure(cause -> {
                    LOGGER.atFatal().log("Loading of management routes was unsuccessful.");
                    promise.fail(cause);
                });

        return promise.future();
    }

    Future<Router> addHealthRouter(final Router root) {
        return RouterBuilder.create(vertx, "openapi-specs/health.yaml")
                     .onSuccess(builder -> {
                         builder.operation("status").handler(rc -> rc.response().end(SUCCESS_RESPONSE));
                         builder.operation("liveness").handler(rc -> rc.response().end(SUCCESS_RESPONSE));
                         root.mountSubRouter("/health", builder.createRouter());
                     }).map(root);
    }

    Future<Router> addMetricsRouter(final Router root) {
        root.get("/metrics").handler(routingContext ->
                routingContext.response()
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(httpMetrics.getRegistry().scrape()));

        return Future.succeededFuture(root);
    }

    private Future<Void> startResourcesServer() {
        final Promise<Void> promise = Promise.promise();
        final Router router = Router.router(vertx);
        router.route().handler(createCORSHander());
        router.route().handler(HSTSHandler.create(Duration.ofDays(365).toSeconds(), false));

        RouterBuilder.create(vertx, "openapi-specs/kafka-admin-rest.yaml")
            .compose(builder -> {
                final Future<Void> result;
                final JsonObject openAPI = builder.getOpenAPI().getOpenAPI();
                final RouterBuilderOptions options = new RouterBuilderOptions();

                // OpenAPI contract document served at `/rest/openapi`
                options.setContractEndpoint(RouterBuilderOptions.STANDARD_CONTRACT_ENDPOINT);
                builder.setOptions(options);

                if (config.isOauthEnabled()) {
                    result = configureOAuth(builder, openAPI);
                } else {
                    openAPI.remove("security");
                    openAPI.getJsonObject("components").remove("securitySchemes");
                    result = Future.succeededFuture();
                }

                assignRoutes(builder);
                router.mountSubRouter("/rest", builder.createRouter());
                return result;
            })
            .compose(nothing -> startResourcesHttpServer(router))
            .onSuccess(promise::complete)
            .onFailure(promise::fail);

        return promise.future();
    }

    private CorsHandler createCORSHander() {
        String allowList = config.getCorsAllowPattern();
        LOGGER.info("CORS allow list regex is {}", allowList);

        return CorsHandler.create(allowList)
                .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                .allowedMethod(io.vertx.core.http.HttpMethod.PATCH)
                .allowedMethod(io.vertx.core.http.HttpMethod.DELETE)
                .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                .allowedHeader("Access-Control-Request-Method")
                .allowedHeader("Access-Control-Allow-Credentials")
                .allowedHeader("Access-Control-Allow-Origin")
                .allowedHeader("Access-Control-Allow-Headers")
                .allowedHeader("Authorization")
                .allowedHeader("Content-Type");
    }

    private Future<Void> configureOAuth(RouterBuilder builder, JsonObject openAPI) {
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

        if (config.getOauthValidIssuerUri() != null) {
            LOGGER.info("JWT issuer (iss) claim valid value: {}", config.getOauthValidIssuerUri());
            oauthOptions.setJWTOptions(new JWTOptions().setIssuer(config.getOauthValidIssuerUri()));
        }

        OAuth2Auth oauth2Provider = OAuth2Auth.create(vertx, oauthOptions);
        OAuth2AuthHandler securityHandler = OAuth2AuthHandler.create(vertx, oauth2Provider);
        builder.securityHandler(SECURITY_SCHEME_NAME, securityHandler);

        JsonObject clientCredentialsFlow = openAPI.getJsonObject("components")
                .getJsonObject("securitySchemes")
                .getJsonObject(SECURITY_SCHEME_NAME)
                .getJsonObject("flows")
                .getJsonObject("clientCredentials");

        if (config.getOauthTokenEndpointUri() != null) {
            LOGGER.info("Setting Oauth2 token endpoint URL: {}", config.getOauthTokenEndpointUri());
            clientCredentialsFlow.put("tokenUrl", config.getOauthTokenEndpointUri());
        } else {
            clientCredentialsFlow.remove("tokenUrl");
        }

        return oauth2Provider.jWKSet()
                .onSuccess(ignored -> LOGGER.info("Loaded JWKS from {}", config.getOauthJwksEndpointUri()))
                .onFailure(cause -> LOGGER.error("Failed to retrieve JWKS: {}", cause.getMessage()));
    }

    private void assignRoutes(final RouterBuilder routerFactory) {
        RestOperations ro = new RestOperations(config, httpMetrics);

        Map<String, Handler<RoutingContext>> routes = Map.of(Operations.GET_TOPIC, ro::describeTopic,
                                                             Operations.GET_TOPICS_LIST, ro::listTopics,
                                                             Operations.DELETE_TOPIC, ro::deleteTopic,
                                                             Operations.CREATE_TOPIC, ro::createTopic,
                                                             Operations.UPDATE_TOPIC, ro::updateTopic,
                                                             Operations.GET_CONSUMER_GROUP, ro::describeGroup,
                                                             Operations.GET_CONSUMER_GROUPS_LIST, ro::listGroups,
                                                             Operations.DELETE_CONSUMER_GROUP, ro::deleteGroup);

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

    private Future<Void> startResourcesHttpServer(Router router) {
        Promise<Void> promise = Promise.promise();
        final String tlsCert = config.getTlsCertificate();
        final HttpServer server;
        final int listenerPort;
        final String portType;

        if (tlsCert == null) {
            server = vertx.createHttpServer();
            listenerPort = HTTP_PORT;
            portType = "plain HTTP";
        } else {
            Set<String> tlsVersions = config.getTlsVersions();
            String tlsKey = config.getTlsKey();

            LOGGER.info("Starting secure admin server with TLS version(s) {}", tlsVersions);

            PemKeyCertOptions certOptions = new PemKeyCertOptions();
            setCertConfig(tlsCert, certOptions::addCertPath, certOptions::addCertValue);
            setCertConfig(tlsKey, certOptions::addKeyPath, certOptions::addKeyValue);

            server = vertx.createHttpServer(new HttpServerOptions()
                                            .setLogActivity(true)
                                            .setSsl(true)
                                            .setEnabledSecureTransportProtocols(tlsVersions)
                                            .setPemKeyCertOptions(certOptions));

            listenerPort = HTTPS_PORT;
            portType = "secure HTTPS";
        }

        server.requestHandler(router).listen(listenerPort)
            .onSuccess(httpServer -> {
                LOGGER.info("Admin Server is listening on {} port {}", portType, listenerPort);
                promise.complete();
            })
            .onFailure(cause -> {
                LOGGER.atFatal().log("Startup of Admin Server resources listener was unsuccessful.");
                promise.fail(cause);
            });

        return promise.future();
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
            } catch (IllegalArgumentException e) {
                LOGGER.info("Cert config value was not base-64 encoded, using raw value. Illegal argument: {}", e.getMessage());
            }

            valueSetter.accept(Buffer.buffer(value));
        }
    }
}
