package org.bf2.admin;

import org.bf2.admin.http.server.AdminServer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    /**
     * Main entrypoint.
     *
     * @param args the command line arguments
     */
    public static void main(final String[] args) throws Exception {
        LoggingConfigWatcher watcher = new LoggingConfigWatcher();
        watcher.initialize();

        LOGGER.info("AdminServer is starting.");

        VertxOptions options = new VertxOptions()
                .setMetricsOptions(
                    new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        final Vertx vertx = Vertx.vertx(options);
        run(vertx)
            .onFailure(throwable -> {
                LOGGER.atFatal().withThrowable(throwable).log("AdminServer startup failed.");
                System.exit(1);
            });
    }

    static Future<String> run(final Vertx vertx) {
        final Promise<String> promise = Promise.promise();
        final AdminServer adminServer = new AdminServer();
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
