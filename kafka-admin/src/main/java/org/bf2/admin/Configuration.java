package org.bf2.admin;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.http.server.AdminServer;

@ApplicationScoped
public class Configuration {

    private static final Logger LOGGER = LogManager.getLogger(Configuration.class);

    @Inject
    Vertx vertx;

    @Inject
    AdminServer adminServer;

    /**
     * Main entrypoint.
     */
    public void start(@Observes StartupEvent event) {
        LOGGER.info("AdminServer is starting.");

        run(vertx)
            .onFailure(throwable -> {
                LOGGER.atFatal().withThrowable(throwable).log("AdminServer startup failed.");
                System.exit(1);
            });
    }

    Future<String> run(final Vertx vertx) {
        final Promise<String> promise = Promise.promise();
        vertx.deployVerticle(adminServer,
            res -> {
                if (res.failed()) {
                    LOGGER.atFatal().withThrowable(res.cause()).log("AdminServer verticle failed to start");
                }
                promise.handle(res);
            }
        );

        return promise.future();
    }
}
