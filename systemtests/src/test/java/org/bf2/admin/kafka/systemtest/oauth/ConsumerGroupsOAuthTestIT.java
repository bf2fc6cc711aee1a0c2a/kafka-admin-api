package org.bf2.admin.kafka.systemtest.oauth;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.bf2.admin.kafka.systemtest.bases.OauthTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.SyncMessaging;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupsOAuthTestIT extends OauthTestBase {

    @Test
    void testConsumerGroupsListAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 2, "localhost:9092", testContext, token);

        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    List<String> resID = MODEL_DESERIALIZER.getGroupsId(buffer);
                    assertThat(resID).hasSameElementsAs(groupIDS);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsListUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 2, "localhost:9092", testContext, token);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.UNAUTHORIZED.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    List<String> resID = MODEL_DESERIALIZER.getGroupsId(buffer);
                    assertThat(resID.size()).isEqualTo(0);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testListWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.getAccessToken().length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.AUTHENTICATION_ERROR.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}
