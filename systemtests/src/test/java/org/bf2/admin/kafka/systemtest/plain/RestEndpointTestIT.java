package org.bf2.admin.kafka.systemtest.plain;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.bases.PlainTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.DynamicWait;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class RestEndpointTestIT extends PlainTestBase {

    @Test
    void testTopicListAfterCreation(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 2; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).hasSameElementsAs(actualRestNames);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        HttpClient client = createHttpClient(vertx);
        deployments.stopKafkaContainer();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                    }
                    assertStrictTransportSecurityDisabled(l.result(), testContext);
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithFilter(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 2; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?filter=test-topic.*")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).isEqualTo(actualRestNames.stream().filter(name -> name.contains("test-topic")).collect(Collectors.toSet()));
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithPagination-{0}")
    @ValueSource(ints = {1, 2, 3})
    void testTopicListWithPagination(int page, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 5; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?size=3&page=" + page)
                .compose(req -> req.send().onSuccess(response -> {
                    // we want to get page 3 of 5 topics. The page size is 3, so we have just 2 pages
                    if (page == 3) {
                        if (response.statusCode() != ReturnCodes.FAILED_REQUEST.code) {
                            testContext.failNow("Status code not correct");
                        }
                    } else if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    // 5 topic, size of page is 3. First page should have 3 topic, second page just 2
                    if (page == 1) {
                        assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(3);
                    }
                    if (page == 2) {
                        assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(2);
                    }
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithLimit-{0}")
    @ValueSource(ints = {1, 2, 3, 5})
    void testTopicListWithLimit(int limit, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?limit=" + limit)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(Math.min(limit, 3));
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithOffset-{0}")
    @ValueSource(ints = {0, 1, 3, 4})
    void testTopicListWithOffset(int offset, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        LOGGER.info("Display name: " + extensionContext.getDisplayName());
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?offset=" + offset)
                .compose(req -> req.send().onSuccess(response -> {
                    if ((response.statusCode() !=  ReturnCodes.SUCCESS.code && offset != 4)
                            || (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code && offset == 4)) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (offset != 4) {
                        assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(3 - offset);
                    }
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithFilterNone(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 2; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?filter=zcfsada.*")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(0);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithSize-{0}")
    @ValueSource(ints = {1, 2, 3, 5})
    void testTopicListWithSize(int size, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?size=" + size)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(Math.min(size, 3));
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithPage-{0}")
    @ValueSource(ints = {1, 3, 4})
    void testTopicListWithPage(int page, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics?page=" + page)
                .compose(req -> req.send().onSuccess(response -> {
                    if ((response.statusCode() !=  ReturnCodes.SUCCESS.code && page == 1)
                            || (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code && page != 1)) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (page == 1) {
                        assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(3);
                    }
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
        createHttpClient(vertx).request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.Topic topic = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.Topic.class);
                    assertThat(topic.getPartitions().size()).isEqualTo(2);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        String queryReq = "/rest/topics/" + topicName;
        deployments.stopKafkaContainer();

        createHttpClient(vertx).request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                        assertStrictTransportSecurityDisabled(l.result(), testContext);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeNonExistingTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = "test-non-exist";

        String queryReq = "/rest/topics/" + topicName;
        createHttpClient(vertx).request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.NOT_FOUND.code) {
                        testContext.failNow("Status code not correct");
                    }
                    assertStrictTransportSecurityDisabled(response, testContext);
                    testContext.completeNow();
                }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.TOPIC_CREATED.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
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
    void testCreateTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);
        deployments.stopKafkaContainer();

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onComplete(l -> testContext.verify(() -> {
                            if (l.succeeded()) {
                                assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                                assertStrictTransportSecurityDisabled(l.result(), testContext);
                            }
                            testContext.completeNow();
                        })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateWithInvJson(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic) + "{./as}").onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateWithInvJson2(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(3);

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send("{" + MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicWithInvName(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        final String topicName = "testTopic3_9-=";
        Types.NewTopic topic = RequestUtils.getTopicObject(topicName, 3);

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateFaultTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "cleanup.policy";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        Types.NewTopicConfigEntry conf = new Types.NewTopicConfigEntry();
        conf.setKey(configKey);
        conf.setValue("true");
        input.setConfig(Collections.singletonList(conf));
        topic.setSettings(input);


        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topic.getName());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateDuplicatedTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        Types.NewTopic topic = RequestUtils.getTopicObject(2);

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topic.getName(), 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topic.getName(), kafkaClient);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.DUPLICATED.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "{displayName}-{0}")
    @ValueSource(ints = { 0, 101 })
    void testCreateTopicWithInvalidNumPartitions(int numPartitions, Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        Types.NewTopic topic = RequestUtils.getTopicObject(numPartitions);

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topic.getName());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteSingle(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        createHttpClient(vertx).request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    DynamicWait.waitForTopicToBeDeleted(topicName, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteWithKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        String query = "/rest/topics/" + topicName;
        deployments.stopKafkaContainer();

        createHttpClient(vertx).request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onComplete(l -> testContext.verify(() -> {
                            if (l.succeeded()) {
                                assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                            }
                            assertStrictTransportSecurityDisabled(l.result(), testContext);
                            testContext.completeNow();
                        })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteNotExisting(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        final String topicName = "test-topic-non-existing";
        String query = "/rest/topics/" + topicName;
        createHttpClient(vertx).request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.NOT_FOUND.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

    }

    @Test
    void testUpdateTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
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
        createHttpClient(vertx).request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
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
    void testUpdateTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));
        deployments.stopKafkaContainer();

        createHttpClient(vertx).request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.KAFKA_DOWN.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testIncreaseTopicPartitions(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.UpdatedTopic topic1 = new Types.UpdatedTopic();
        topic1.setName(topicName);
        Types.NewTopicConfigEntry conf = new Types.NewTopicConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));
        topic1.setNumPartitions(3);

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        createHttpClient(vertx).request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    DynamicWait.waitForTopicExists(topicName, kafkaClient);
                    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                            topicName);
                    String configVal = kafkaClient.describeConfigs(Collections.singletonList(resource))
                            .all().get().get(resource).get("min.insync.replicas").value();
                    assertThat(configVal).isEqualTo("2");
                    int partitions = kafkaClient.describeTopics(Collections.singletonList(topicName)).all().get().get(topicName).partitions().size();
                    assertThat(partitions).isEqualTo(3);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDecreaseTopicPartitions(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        final String topicName = UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.UpdatedTopic topic1 = new Types.UpdatedTopic();
        topic1.setName(topicName);
        Types.NewTopicConfigEntry conf = new Types.NewTopicConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));
        topic1.setNumPartitions(1);

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 3, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        createHttpClient(vertx).request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.FAILED_REQUEST.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityDisabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}