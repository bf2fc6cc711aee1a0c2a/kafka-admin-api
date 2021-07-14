package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Tag(TestTag.PLAIN)
public class PlainTestBase extends TestBase {

    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static DeploymentManager deployments;

    protected GenericContainer<?> kafkaContainer;
    protected GenericContainer<?> adminContainer;

    protected int publishedAdminPort;
    protected AdminClient kafkaClient;

    @BeforeAll
    static void initialize(ExtensionContext extensionContext) {
        deployments = DeploymentManager.newInstance(extensionContext, false);
    }

    @AfterAll
    static void shutdown() {
        deployments.shutdown();
    }

    @BeforeEach
    void setup() {
        kafkaContainer = deployments.getKafkaContainer();
        adminContainer = deployments.getAdminContainer();
    }

    @BeforeEach
    void setup(Vertx vertx) {
        this.publishedAdminPort = deployments.getAdminServerPort();
        this.kafkaClient = deployments.createKafkaAdmin();
    }

    @AfterEach
    void cleanup(Vertx vertx) throws InterruptedException, ExecutionException {
        Future.succeededFuture(deployments.createKafkaAdmin())
            .map(adminClient -> adminClient.listTopics()
                 .listings()
                 .whenComplete((allTopics, error) -> {
                     adminClient.deleteTopics(allTopics.stream().map(TopicListing::name).collect(Collectors.toList()));
                 }))
            .toCompletionStage()
            .toCompletableFuture()
            .get();

        if (this.kafkaClient != null) {
            this.kafkaClient.close();
        }
    }

    protected HttpClient createHttpClient(Vertx vertx) {
        return super.createHttpClient(vertx, false);
    }

    protected void assertStrictTransportSecurityDisabled(HttpClientResponse response, VertxTestContext testContext) {
        assertStrictTransportSecurity(response, testContext, false);
    }
}
