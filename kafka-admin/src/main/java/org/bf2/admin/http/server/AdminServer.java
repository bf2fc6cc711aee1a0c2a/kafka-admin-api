package org.bf2.admin.http.server;


import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.Operations;
import org.bf2.admin.kafka.admin.handlers.RestOperations;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Set;
import java.util.function.Consumer;

/**
 * The main Kafka Admin API Server class. It is a Vert.x {@link io.vertx.core.Verticle} and it starts
 * an HTTP server which listen for inbound HTTP requests.
 * <p>
 * The routes are split into two groups, management and resources. Management routes include metrics and
 * health checks and are exposed in clear text on port 8080. Resource routes include the Kafka entities
 * accessed by users of the admin server and are exposed using TLS on port 8443.
 */
public class AdminServer extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger(AdminServer.class);
    private static final String SUCCESS_RESPONSE = "{\"status\": \"OK\"}";
    private static final String DEFAULT_TLS_VERSION = "TLSv1.3";
    private static final Decoder BASE64_DECODER = Base64.getDecoder();

    private HttpMetrics httpMetrics = new HttpMetrics();

    @Override
    public void start(final Promise<Void> startServer) {
        getManagementRouter()
            .onSuccess(router -> {
                final HttpServer server = vertx.createHttpServer();
                server.requestHandler(router).listen(8080);
                LOGGER.info("Admin Server management is listening on port 8080");
            })
            .onFailure(throwable -> LOGGER.atFatal().withThrowable(throwable).log("Loading of management routes was unsuccessful."));

        getResourcesRouter()
            .onSuccess(router -> startSecureHttpServer(startServer, router))
            .onFailure(throwable -> LOGGER.atFatal().withThrowable(throwable).log("Loading of routes was unsuccessful."));
    }

    private Future<Router> getManagementRouter() {
        return addHealthRouter(Router.router(vertx))
                .compose(this::addMetricsRouter);
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
        root.get("/rest/metrics").handler(routingContext ->
                routingContext.response()
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(httpMetrics.getRegistry().scrape()));

        return Future.succeededFuture(root);
    }

    private Future<Router> getResourcesRouter() {
        final Router router = Router.router(vertx);
        router.route().handler(createCORSHander());

        final Promise<Router> promise = Promise.promise();

        return RouterBuilder.create(vertx, "openapi-specs/rest.yaml")
            .onSuccess(builder -> {
                assignRoutes(builder, vertx);
                router.mountSubRouter("/rest", builder.createRouter());
            }).onFailure(promise::fail)
            .map(router);
    }

    private CorsHandler createCORSHander() {
        String defaultAllowRegex = "(https?:\\/\\/localhost(:\\d*)?)";
        String envAllowList = System.getenv("CORS_ALLOW_LIST_REGEX");
        String allowList = envAllowList == null ? defaultAllowRegex : envAllowList;
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

    private void assignRoutes(final RouterBuilder routerFactory, final Vertx vertx) {
        RestOperations ro = new RestOperations(httpMetrics);

        routerFactory.operation(Operations.GET_TOPIC).handler(ro::describeTopic).failureHandler(ro::errorHandler);
        routerFactory.operation(Operations.GET_TOPICS_LIST).handler(ro::listTopics).failureHandler(ro::errorHandler);

        routerFactory.operation(Operations.DELETE_TOPIC).handler(ro::deleteTopic).failureHandler(ro::errorHandler);
        routerFactory.operation(Operations.CREATE_TOPIC).handler(ro::createTopic).failureHandler(ro::errorHandler);
        routerFactory.operation(Operations.UPDATE_TOPIC).handler(ro::updateTopic).failureHandler(ro::errorHandler);

        routerFactory.operation(Operations.GET_CONSUMER_GROUP).handler(ro::describeGroup).failureHandler(ro::errorHandler);
        routerFactory.operation(Operations.GET_CONSUMER_GROUPS_LIST).handler(ro::listGroups).failureHandler(ro::errorHandler);

        routerFactory.operation(Operations.DELETE_CONSUMER_GROUP).handler(ro::deleteGroup).failureHandler(ro::errorHandler);
    }

    private void startSecureHttpServer(final Promise<Void> startServer, Router router) {
        final String[] tlsVersions = System.getenv().getOrDefault("KAFKA_ADMIN_TLS_VERSION", DEFAULT_TLS_VERSION).split(",");
        final String tlsCert = System.getenv("KAFKA_ADMIN_TLS_CERT");
        final String tlsKey = System.getenv("KAFKA_ADMIN_TLS_KEY");

        if (tlsCert == null || tlsKey == null) {
            startServer.fail("TLS certificate and/or key is missing");
        } else {
            LOGGER.info("Starting secure admin server with TLS version(s) {}", (Object) tlsVersions);
            PemKeyCertOptions certOptions = new PemKeyCertOptions();

            setCertConfig(tlsCert, certOptions::addCertPath, certOptions::addCertValue);
            setCertConfig(tlsKey, certOptions::addKeyPath, certOptions::addKeyValue);

            final HttpServer server = vertx.createHttpServer(new HttpServerOptions()
                                                             .setLogActivity(true)
                                                             .setSsl(true)
                                                             .setEnabledSecureTransportProtocols(Set.of(tlsVersions))
                                                             .setPemKeyCertOptions(certOptions));

            server.requestHandler(router).listen(8443);
            LOGGER.info("Admin Server is listening on port 8443");
        }
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
                LOGGER.warn("Cert config value was not base-64 encoded, using raw value. Illegal argument: {}", e.getMessage());
            }

            valueSetter.accept(Buffer.buffer(value));
        }
    }
}
