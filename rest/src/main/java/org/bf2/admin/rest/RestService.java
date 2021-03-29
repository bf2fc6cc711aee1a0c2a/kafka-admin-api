package org.bf2.admin.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.bf2.admin.http.server.registration.RouteRegistration;
import org.bf2.admin.http.server.registration.RouteRegistrationDescriptor;
import org.bf2.admin.kafka.admin.HttpMetrics;
import org.bf2.admin.kafka.admin.KafkaAdminConfigRetriever;
import org.bf2.admin.kafka.admin.Operations;
import org.bf2.admin.kafka.admin.handlers.RestOperations;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Implements routes to be used as kubernetes liveness and readiness probes. The implementations
 * simply return a static string containing a JSON body of "status: ok".
 */
public class RestService implements RouteRegistration {

    protected final Logger log = LogManager.getLogger(RestService.class);
    KafkaAdminConfigRetriever kaConfig;
    HttpMetrics httpMetrics = new HttpMetrics();

    @Override
    public Future<RouteRegistrationDescriptor> getRegistrationDescriptor(final Vertx vertx) {

        final Promise<RouteRegistrationDescriptor> promise = Promise.promise();

        RouterBuilder.create(vertx, "openapi-specs/rest.yaml", ar -> {
            if (ar.succeeded()) {
                try {
                    kaConfig = new KafkaAdminConfigRetriever();
                } catch (Exception e) {
                    promise.fail(e);
                    return;
                }
                RouterBuilder routerBuilder = ar.result();
                assignRoutes(routerBuilder, vertx);
                promise.complete(RouteRegistrationDescriptor.create("/rest", routerBuilder.createRouter()));
                log.info("Rest server version {} started.", RestService.class.getPackage().getImplementationVersion());
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    private void assignRoutes(final RouterBuilder routerFactory, final Vertx vertx) {
        RestOperations ro = new RestOperations();
        routerFactory.operation(Operations.GET_TOPIC).handler(ro.describeTopic(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));
        routerFactory.operation(Operations.GET_TOPICS_LIST).handler(ro.listTopics(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));

        routerFactory.operation(Operations.DELETE_TOPIC).handler(ro.deleteTopic(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));
        routerFactory.operation(Operations.CREATE_TOPIC).handler(ro.createTopic(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));
        routerFactory.operation(Operations.UPDATE_TOPIC).handler(ro.updateTopic(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));

        routerFactory.operation(Operations.GET_CONSUMER_GROUP).handler(ro.describeGroup(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));
        routerFactory.operation(Operations.GET_CONSUMER_GROUPS_LIST).handler(ro.listGroups(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));

        routerFactory.operation(Operations.DELETE_CONSUMER_GROUP).handler(ro.deleteGroup(kaConfig, vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));

        routerFactory.operation(Operations.OPEN_API).handler(ro.openApi(vertx, httpMetrics)).failureHandler(ro.errorHandler(httpMetrics));

        routerFactory.operation(Operations.METRICS).handler(routingContext -> routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end(httpMetrics.getRegistry().scrape()));


    }
}
