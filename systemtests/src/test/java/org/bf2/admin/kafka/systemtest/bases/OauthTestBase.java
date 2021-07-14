package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager.UserType;
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

@Tag(TestTag.OAUTH)
public class OauthTestBase extends TestBase {

    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static DeploymentManager deployments;

    protected GenericContainer<?> kafkaContainer;
    protected GenericContainer<?> adminContainer;

    protected int publishedAdminPort;
    protected AdminClient kafkaClient;
    protected String token;
    protected String externalBootstrap;

    @BeforeAll
    static void initialize(ExtensionContext extensionContext) {
        deployments = DeploymentManager.newInstance(extensionContext, true);
        deployments.getKeycloakContainer();
    }

    @AfterAll
    static void shutdown() {
        deployments.shutdown();
    }

    @BeforeEach
    void setup() {
        kafkaContainer = deployments.getKafkaContainer();
        adminContainer = deployments.getAdminContainer();
        externalBootstrap = deployments.getExternalBootstrapServers();
    }

    @BeforeEach
    void setup(Vertx vertx) {
        this.publishedAdminPort = deployments.getAdminServerPort();
        this.token = deployments.getAccessTokenNow(vertx, UserType.OWNER);
        this.kafkaClient = deployments.createKafkaAdmin(token);
    }

    @AfterEach
    void cleanup(Vertx vertx) throws InterruptedException, ExecutionException {
        deployments.getAccessToken(vertx, UserType.OWNER)
            .map(token -> deployments.createKafkaAdmin(token))
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
        return super.createHttpClient(vertx, true);
    }

    protected void changeTokenToUnauthorized(Vertx vertx, VertxTestContext testContext) {
        this.token = deployments.getAccessTokenNow(vertx, UserType.OTHER);
    }

    protected void assertStrictTransportSecurityEnabled(HttpClientResponse response, VertxTestContext testContext) {
        assertStrictTransportSecurity(response, testContext, true);
    }

}
