package org.bf2.admin.kafka.systemtest.oauth;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.bases.OauthTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.SyncMessaging;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupsOAuthTestIT extends OauthTestBase {

    @Test
    void testConsumerGroupsListAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.ConsumerGroupList response = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class);
                    List<String> responseGroupIDs = response.getItems().stream().map(cg -> cg.getGroupId()).collect(Collectors.toList());
                    assertThat(groupIDS).hasSameElementsAs(responseGroupIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsListUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.SUCCESS.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsListWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, token);
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsDescribeAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, token);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct. Got: " + response.statusCode() + "expected: " + ReturnCodes.SUCCESS.code);
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    ConsumerGroupDescription description = kafkaClient.describeConsumerGroups(Collections.singletonList(groupIDS.get(0))).describedGroups().get(groupIDS.get(0)).get();
                    Map<TopicPartition, OffsetAndMetadata> assignedPartitions = kafkaClient.listConsumerGroupOffsets(groupIDS.get(0)).partitionsToOffsetAndMetadata().get();
                    Types.ConsumerGroupDescription cG = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupDescription.class);
                    assertThat(cG.getConsumers().size()).isEqualTo(assignedPartitions.size());
                    assertThat(cG.getState()).isEqualTo(description.state().name());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsDescribeUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, token);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.FORBIDDEN.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsDeleteAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.GROUP_DELETED.code) {
                        testContext.failNow("Status code not correct, was: " + response.statusCode() + " expected: " + ReturnCodes.GROUP_DELETED.code);
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    groupIDS.remove(0);
                    assertThat(testContext.failed()).isFalse();
                    List<String> consumerGroups = kafkaClient.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    assertThat(consumerGroups).hasSameElementsAs(groupIDS);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testConsumerGroupsDeleteUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + token).send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.FORBIDDEN.code) {
                        testContext.failNow("Status code " + response.statusCode() + " is not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    List<String> ids = kafkaClient.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    assertThat(ids).hasSameElementsAs(groupIDS);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDeleteWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> groupIDS = SyncMessaging.createConsumerGroups(kafkaClient, 2, externalBootstrap, testContext, token);
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupIDS.get(0))
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

}
