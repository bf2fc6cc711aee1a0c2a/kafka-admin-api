package bf.admin.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import bf.admin.http.server.registration.RouteRegistration;
import bf.admin.http.server.registration.RouteRegistrationDescriptor;
import admin.kafka.admin.HttpMetrics;
import admin.kafka.admin.KafkaAdminConfigRetriever;
import admin.kafka.admin.Operations;
import admin.kafka.admin.handlers.RestOperations;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
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

        OpenAPI3RouterFactory.create(vertx, "openapi-specs/rest.yaml", ar -> {
            if (ar.succeeded()) {
                try {
                    kaConfig = new KafkaAdminConfigRetriever();
                } catch (Exception e) {
                    promise.fail(e);
                }
                OpenAPI3RouterFactory routerFactory = ar.result();
                assignRoutes(routerFactory, vertx);
                promise.complete(RouteRegistrationDescriptor.create("/rest", routerFactory.getRouter()));
                log.info("Rest server started.");
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    private void assignRoutes(final OpenAPI3RouterFactory routerFactory, final Vertx vertx) {
        RestOperations ro = new RestOperations();
        routerFactory.addHandlerByOperationId(Operations.GET_TOPIC, ro.describeTopic(kaConfig, vertx, httpMetrics));
        routerFactory.addHandlerByOperationId(Operations.GET_TOPICS_LIST, ro.listTopics(kaConfig, vertx, httpMetrics));

        routerFactory.addHandlerByOperationId(Operations.DELETE_TOPIC, ro.deleteTopic(kaConfig, vertx, httpMetrics));
        routerFactory.addHandlerByOperationId(Operations.CREATE_TOPIC, ro.createTopic(kaConfig, vertx, httpMetrics));
        routerFactory.addHandlerByOperationId(Operations.UPDATE_TOPIC, ro.updateTopic(kaConfig, vertx, httpMetrics));

        routerFactory.addHandlerByOperationId(Operations.GET_CONSUMER_GROUP, ro.describeGroup(kaConfig, vertx, httpMetrics));
        routerFactory.addHandlerByOperationId(Operations.GET_CONSUMER_GROUPS_LIST, ro.listGroups(kaConfig, vertx, httpMetrics));

        routerFactory.addHandlerByOperationId(Operations.DELETE_CONSUMER_GROUP, ro.deleteGroup(kaConfig, vertx, httpMetrics));

        routerFactory.addHandlerByOperationId(Operations.OPEN_API, ro.openApi(vertx, httpMetrics));

        routerFactory.addHandlerByOperationId(Operations.METRICS, routingContext -> routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end(httpMetrics.getRegistry().scrape()));


        routerFactory.addFailureHandlerByOperationId(Operations.GET_TOPIC, ro.errorHandler(httpMetrics));
        routerFactory.addFailureHandlerByOperationId(Operations.GET_TOPICS_LIST, ro.errorHandler(httpMetrics));

        routerFactory.addFailureHandlerByOperationId(Operations.DELETE_TOPIC, ro.errorHandler(httpMetrics));
        routerFactory.addFailureHandlerByOperationId(Operations.CREATE_TOPIC, ro.errorHandler(httpMetrics));
        routerFactory.addFailureHandlerByOperationId(Operations.UPDATE_TOPIC, ro.errorHandler(httpMetrics));

        routerFactory.addFailureHandlerByOperationId(Operations.GET_CONSUMER_GROUP, ro.errorHandler(httpMetrics));
        routerFactory.addFailureHandlerByOperationId(Operations.GET_CONSUMER_GROUPS_LIST, ro.errorHandler(httpMetrics));

        routerFactory.addFailureHandlerByOperationId(Operations.DELETE_CONSUMER_GROUP, ro.errorHandler(httpMetrics));
    }
}
