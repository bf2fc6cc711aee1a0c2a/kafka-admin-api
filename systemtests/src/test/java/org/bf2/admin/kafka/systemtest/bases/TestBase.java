package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.IndicativeSentences;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import org.bf2.admin.kafka.systemtest.listeners.ExtensionContextParameterResolver;
import org.bf2.admin.kafka.systemtest.listeners.TestCallbackListener;
import org.bf2.admin.kafka.systemtest.listeners.TestExceptionCallbackListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@ExtendWith(TestCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(VertxExtension.class)
@DisplayNameGeneration(IndicativeSentences.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(TestBase.class);

    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static DeploymentManager deployments;

    protected GenericContainer<?> kafkaContainer;
    protected GenericContainer<?> adminContainer;

    protected int publishedAdminPort;
    protected AdminClient kafkaClient;
    protected String externalBootstrap;

    @AfterAll
    static void shutdown() {
        deployments.shutdown();
    }

    @BeforeEach
    void setup() {
        kafkaContainer = deployments.getKafkaContainer();
        adminContainer = deployments.getAdminContainer();
        externalBootstrap = deployments.getExternalBootstrapServers();
        publishedAdminPort = deployments.getAdminServerPort();
    }

    @AfterEach
    void cleanup(Vertx vertx) {
        if (this.kafkaClient != null) {
            this.kafkaClient.close();
        }
    }

    protected void deleteAllTopics() throws InterruptedException, ExecutionException {
        Future.succeededFuture(kafkaClient)
            .compose(adminClient -> {
                Promise<Void> promise = Promise.promise();

                adminClient.listTopics()
                    .listings()
                    .whenComplete((allTopics, listError) -> {
                        if (listError != null) {
                            promise.fail(listError);
                        } else {
                            adminClient.deleteTopics(allTopics.stream().map(TopicListing::name).collect(Collectors.toList()))
                                .all()
                                .whenComplete((nothing, deleteError) -> {
                                    if (deleteError != null) {
                                        promise.fail(deleteError);
                                    } else {
                                        promise.complete();
                                    }
                                });
                        }
                    });

                return promise.future();
            })
            .toCompletionStage()
            .toCompletableFuture()
            .get();
    }

    protected HttpClient createHttpClient(Vertx vertx) {
        return createHttpClient(vertx, deployments.isOauthEnabled());
    }

    protected void assertStrictTransportSecurityEnabled(HttpClientResponse response, VertxTestContext testContext) {
        assertStrictTransportSecurity(response, testContext, true);
    }

    protected void assertStrictTransportSecurityDisabled(HttpClientResponse response, VertxTestContext testContext) {
        assertStrictTransportSecurity(response, testContext, false);
    }

    protected HttpClient createHttpClient(Vertx vertx, boolean isSecured) {
        HttpClientOptions options = new HttpClientOptions();
        if (isSecured) {
            options.setSsl(true);
            options.setEnabledSecureTransportProtocols(Set.of("TLSv1.3"));
            options.setPemTrustOptions(new PemTrustOptions().addCertPath(Path.of("docker", "certificates", "ca.crt").toAbsolutePath().toString()));
        }
        return vertx.createHttpClient(options);
    }

    protected void assertStrictTransportSecurity(HttpClientResponse response, VertxTestContext testContext, boolean secureTransport) {
        String hsts = response.getHeader("Strict-Transport-Security");

        if (secureTransport) {
            if (hsts == null) {
                testContext.failNow("HSTS header missing");
            } else if (!hsts.equals(String.format("max-age=%d", Duration.ofDays(365).toSeconds()))) {
                testContext.failNow("HSTS header unexpected value");
            }
        } else if (hsts != null) {
            testContext.failNow("HSTS header present on insecure response");
        }
    }

}
