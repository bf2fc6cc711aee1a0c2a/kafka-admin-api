package admin.kafka.systemtest.bases;

import admin.kafka.systemtest.TestTag;
import admin.kafka.systemtest.json.ModelDeserializer;
import admin.kafka.systemtest.json.TokenModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Tag(TestTag.OAUTH)
public class OauthTestBase extends TestBase {
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    protected static TokenModel token = new TokenModel();
    protected AdminClient kafkaClient;
    protected int publishedAdminPort = 0;

    @BeforeEach
    public void startup(Vertx vertx, VertxTestContext vertxTestContext, ExtensionContext extensionContext) throws Exception {
        DEPLOYMENT_MANAGER.deployOauthStack(vertxTestContext, extensionContext);
        publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        // Get valid auth token
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
        createKafkaAdmin();
    }

    private void createKafkaAdmin() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"" + token.getAccessToken() + "\";");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        kafkaClient = KafkaAdminClient.create(props);
    }

    protected void changeTokenToUnauthorized(Vertx vertx, VertxTestContext testContext) {
        HttpClient client = vertx.createHttpClient();
        String payload = "grant_type=client_credentials&client_secret=team-a-client-secret&client_id=team-a-client";
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
        kafkaClient.close();
        kafkaClient = null;
    }
}
