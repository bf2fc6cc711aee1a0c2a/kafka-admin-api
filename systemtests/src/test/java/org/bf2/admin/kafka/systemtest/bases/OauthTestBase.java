package org.bf2.admin.kafka.systemtest.bases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import org.bf2.admin.kafka.systemtest.json.TokenModel;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Tag(TestTag.OAUTH)
public class OauthTestBase extends TestBase {
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static TokenModel token = new TokenModel();
    protected AdminClient kafkaClient;
    protected int publishedAdminPort = 0;

    @BeforeAll
    public static void initialize(Vertx vertx, VertxTestContext vertxTestContext, ExtensionContext extensionContext) throws Exception {
        DEPLOYMENT_MANAGER.deployKeycloak(vertxTestContext, extensionContext);
    }

    @AfterAll
    public static void cleanup(ExtensionContext extensionContext) throws Exception {
        DEPLOYMENT_MANAGER.teardown(extensionContext);
    }

    @BeforeEach
    public void startup(Vertx vertx, VertxTestContext vertxTestContext, ExtensionContext extensionContext) throws Exception {
        DEPLOYMENT_MANAGER.deployOauthStack(vertxTestContext, extensionContext);
        publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        // Get valid auth token
        //String payload = "grant_type=client_credentials&client_id=kafka&client_secret=kafka-secret";
        changeTokenToAuthorized(vertx, vertxTestContext);
        createKafkaAdmin();
    }

    protected HttpClient createHttpClient(Vertx vertx) {
        return super.createHttpClient(vertx, true);
    }

    private void createKafkaAdmin() {
        kafkaClient = KafkaAdminClient.create(ClientsConfig.getAdminConfigOauth(token));
    }

    protected void changeTokenToAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        HttpClient client = vertx.createHttpClient();
        String payload = "grant_type=password&username=alice&password=alice-password&client_id=kafka-cli";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        client.request(HttpMethod.POST, 8080, "localhost", "/auth/realms/kafka-authz/protocol/openid-connect/token")
                .compose(req -> req.putHeader("Host", "keycloak:8080")
                        .putHeader("Content-Type", "application/x-www-form-urlencoded").send(payload))
                .compose(HttpClientResponse::body).onComplete(buffer -> {
                    try {
                        token = new ObjectMapper().readValue(buffer.result().toString(), TokenModel.class);
                        LOGGER.warn("Got token");
                        countDownLatch.countDown();
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });
        countDownLatch.await(30, TimeUnit.SECONDS);
    }

    protected void changeTokenToUnauthorized(Vertx vertx, VertxTestContext testContext) {
        HttpClient client = vertx.createHttpClient();
        String payload = "grant_type=password&username=bob&password=bob-password&client_id=kafka-cli";
        CountDownLatch countDownLatch = new CountDownLatch(1);
        client.request(HttpMethod.POST, 8080, "localhost", "/auth/realms/kafka-authz/protocol/openid-connect/token")
                .compose(req -> req.putHeader("Host", "keycloak:8080")
                        .putHeader("Content-Type", "application/x-www-form-urlencoded").send(payload))
                .compose(HttpClientResponse::body).onComplete(buffer -> {
                    try {
                        token = new ObjectMapper().readValue(buffer.result().toString(), TokenModel.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            testContext.failNow("Could not retrieve token");
            testContext.completeNow();
        }
    }

    @AfterEach
    public void teardown() {
        if (kafkaClient != null) {
            kafkaClient.close();
            kafkaClient = null;
        }
    }
}
