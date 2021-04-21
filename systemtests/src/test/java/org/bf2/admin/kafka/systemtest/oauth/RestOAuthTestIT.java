package org.bf2.admin.kafka.systemtest.oauth;

import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.DynamicWait;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.bf2.admin.kafka.systemtest.bases.OauthTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestOAuthTestIT extends OauthTestBase {
    @Test
    public void testListWithValidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(UUID.randomUUID().toString());
        topicNames.add(UUID.randomUUID().toString());

        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(topicNames.get(0), 1, (short) 1),
                new NewTopic(topicNames.get(1), 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(topicNames, kafkaClient);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).hasSameElementsAs(actualRestNames);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testListUnauthorizedUser(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(UUID.randomUUID().toString());
        topicNames.add(UUID.randomUUID().toString());

        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(topicNames.get(0), 1, (short) 1),
                new NewTopic(topicNames.get(1), 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(topicNames, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(0);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

    }

    @Disabled
    @Test
    public void testListWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(UUID.randomUUID().toString());
        topicNames.add(UUID.randomUUID().toString());

        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(topicNames.get(0), 1, (short) 1),
                new NewTopic(topicNames.get(1), 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(topicNames, kafkaClient);
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.getAccessToken().length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        }))
                        .onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testListWithExpiredToken(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        // Wait for token to expire
        Thread.sleep(120_000);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.Topic topic = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.Topic.class);
                    assertThat(topic.getPartitions().size()).isEqualTo(2);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);

        vertx.createHttpClient().request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.TOPIC_CREATED.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(topic.getName(), kafkaClient);
                    TopicDescription description = kafkaClient.describeTopics(Collections.singleton(topic.getName()))
                            .all().get().get(topic.getName());
                    assertThat(description.isInternal()).isEqualTo(false);
                    assertThat(description.partitions().size()).isEqualTo(3);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicUnauthorizedIncrementsMetric(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);
        HttpClient httpClient = vertx.createHttpClient();
        AtomicLong unauthorizedRequestCountBefore = new AtomicLong(0L);

        Arrays.stream(RequestUtils.retrieveMetrics(testContext, httpClient, publishedAdminPort).split("\n"))
            .filter(line -> line.startsWith("failed_requests_total{status_code=\"401\",}"))
            .map(line -> Double.valueOf(line.split("\\s+")[1]).longValue())
            .forEach(value -> unauthorizedRequestCountBefore.addAndGet(value));

        httpClient.request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

        AtomicLong unauthorizedRequestCountAfter = new AtomicLong(0L);

        Arrays.stream(RequestUtils.retrieveMetrics(testContext, httpClient, publishedAdminPort).split("\n"))
            .filter(line -> line.startsWith("failed_requests_total{status_code=\"401\",}"))
            .map(line -> Double.valueOf(line.split("\\s+")[1]).longValue())
            .forEach(value -> unauthorizedRequestCountAfter.addAndGet(value));

        assertTrue(unauthorizedRequestCountBefore.get() < unauthorizedRequestCountAfter.get());
    }

    @Test
    void testTopicDeleteSingleAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send().onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicToBeDeleted(topicName, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteSingleUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send().onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testUpdateTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(topicName, kafkaClient);
                    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                            topicName);
                    String configVal = kafkaClient.describeConfigs(Collections.singletonList(resource))
                            .all().get().get(resource).get("min.insync.replicas").value();
                    assertThat(configVal).isEqualTo("2");
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testUpdateTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testIndependenceOfRequests(Vertx vertx, VertxTestContext testContext) throws Exception {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(UUID.randomUUID().toString());
        topicNames.add(UUID.randomUUID().toString());

        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(topicNames.get(0), 1, (short) 1),
                new NewTopic(topicNames.get(1), 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(topicNames, kafkaClient);
        HttpClient client = vertx.createHttpClient();
        CountDownLatch latch = new CountDownLatch(1);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).hasSameElementsAs(actualRestNames);
                    latch.countDown();
                })));
        latch.await(1, TimeUnit.MINUTES);

        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.send().onSuccess(response -> testContext.verify(() -> {
                    assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}