package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.exceptions.ContractViolationException;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import org.bf2.admin.kafka.systemtest.listeners.ExtensionContextParameterResolver;
import org.bf2.admin.kafka.systemtest.listeners.TestCallbackListener;
import org.bf2.admin.kafka.systemtest.listeners.TestExceptionCallbackListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@ExtendWith(TestCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(VertxExtension.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(TestBase.class);

    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static DeploymentManager deployments;

    protected GenericContainer<?> kafkaContainer;
    protected GenericContainer<?> adminContainer;
    protected GenericContainer<?> proxyContainer;

    protected int publishedAdminPort;
    protected AdminClient kafkaClient;
    protected String externalBootstrap;

    @AfterAll
    static void shutdown() {
        deployments.shutdown();
    }

    @BeforeEach
    void setup(ExtensionContext context, VertxTestContext vertxContext) {
        try {
            kafkaContainer = deployments.getKafkaContainer();
            adminContainer = deployments.getAdminContainer();
            externalBootstrap = deployments.getExternalBootstrapServers();
            publishedAdminPort = deployments.getAdminServerPort();
            proxyContainer = deployments.getProxyContainer();
            vertxContext.completeNow();
        } catch (Exception e) {
            vertxContext.failNow(e);
        }
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
            Buffer caCert;
            try (InputStream stream = getClass().getResourceAsStream("/certs/ca.crt")) {
                caCert = Buffer.buffer(new BufferedReader(new InputStreamReader(stream))
                    .lines().collect(Collectors.joining("\n")));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            options.setPemTrustOptions(new PemTrustOptions().addCertValue(caCert));
        }
        return vertx.createHttpClient(options);
    }

    protected void assertCorrectResponseCode(int actual, int expected, VertxTestContext testContext) {
        if (actual != expected) {
            testContext.failNow("Status code '" + actual + "' is not correct, expected '" + expected + "'");
        }
    }

    protected void assertContractViolationErrorOrInvalidResponseCode(HttpClientResponse response, int expectedCode, VertxTestContext testContext) {
        response.body().onComplete(ar -> {
            JsonObject responseObj;
            if (ar.succeeded()) {
                try {
                    responseObj = ar.result().toJsonObject();
                } catch (Exception e) {
                    testContext.failNow(ar.result().toString());
                    return;
                }
                String type = responseObj.getString("type");
                if (type != null && type.startsWith("https://stoplight.io/prism/errors")) {
                    String title = responseObj.getString("title");
                    String validation = responseObj.getString("validation");
                    String message = String.format("%s: %s", title, validation);
                    testContext.failNow(new ContractViolationException(message));
                }
            }
            assertCorrectResponseCode(response.statusCode(), expectedCode, testContext);
        });
    }

    // Fail test if the response violates the OpenAPI contract
    protected void assertContractViolationErrorFromResponse(HttpClientResponse response, VertxTestContext testContext) {
        response.body().onComplete(ar -> {
            JsonObject responseObj;
            if (ar.succeeded()) {
                try {
                    responseObj = ar.result().toJsonObject();
                } catch (Exception e) {
                    testContext.failNow(ar.result().toString());
                    return;
                }
                String type = responseObj.getString("type");
                if (type != null && type.startsWith("https://stoplight.io/prism/errors")) {
                    String title = responseObj.getString("title");
                    String validation = responseObj.getString("validation");
                    String message = String.format("%s: %s", title, validation);
                    testContext.failNow(new ContractViolationException(message));
                }
            }
        });
    }

    // Fail test if the response violates the OpenAPI contract
    protected void assertContractViolationFromBuffer(Buffer buffer, VertxTestContext testContext) {
        JsonObject responseObj = buffer.toJsonObject();
        String type = responseObj.getString("type");
        if (type != null && type.startsWith("https://stoplight.io/prism/errors")) {
            String title = responseObj.getString("title");
            String validation = responseObj.getString("validation");
            String message = String.format("%s: %s", title, validation);
            testContext.failNow(new ContractViolationException(message));
        }
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
