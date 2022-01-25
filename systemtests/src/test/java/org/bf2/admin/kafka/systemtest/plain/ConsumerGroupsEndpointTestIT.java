package org.bf2.admin.kafka.systemtest.plain;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.bases.PlainTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.AsyncMessaging;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.bf2.admin.kafka.systemtest.utils.DynamicWait;
import org.bf2.admin.kafka.systemtest.utils.SyncMessaging;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class ConsumerGroupsEndpointTestIT extends PlainTestBase {

    @Test
    void testListConsumerGroups(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        SyncMessaging.createConsumerGroups(kafkaClient, 5, deployments.getExternalBootstrapServers(), testContext);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    List<String> consumerGroups = kafkaClient.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    Types.ConsumerGroupList response = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class);
                    response.getItems().forEach(item -> item.getConsumers().forEach(consumer -> assertThat(consumer.getMemberId()).isNull()));
                    List<String> responseGroupIDs = response.getItems().stream().map(cg -> cg.getGroupId()).collect(Collectors.toList());
                    assertThat(consumerGroups).hasSameElementsAs(responseGroupIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testEmptyTopicsOnList(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        SyncMessaging.createConsumerGroups(kafkaClient, 4, deployments.getExternalBootstrapServers(), testContext);
        String topic = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(new NewTopic(topic, 3, (short) 1)));
        DynamicWait.waitForTopicsExists(Collections.singletonList(topic), kafkaClient);

        Properties props = ClientsConfig.getConsumerConfig(deployments.getExternalBootstrapServers(), "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);

        AsyncMessaging.produceMessages(vertx, deployments.getExternalBootstrapServers(), topic, 30, null);
        consumer.subscribe(topic);
        AtomicReference<KafkaConsumerRecords<String, String>> records = new AtomicReference<>();
        CountDownLatch cd = new CountDownLatch(1);
        consumer.poll(Duration.ofSeconds(60), result -> {
            if (!result.result().isEmpty()) {
                cd.countDown();
                records.set(result.result());
            }
        });
        assertThat(cd.await(80, TimeUnit.SECONDS)).isTrue();
        consumer.close();

        HttpClient client = createHttpClient(vertx);
        CircuitBreaker breaker = CircuitBreaker.create("rebalance-waiter", vertx, new CircuitBreakerOptions()
                .setTimeout(2000).setResetTimeout(3000).setMaxRetries(60)).retryPolicy(retryCount -> retryCount * 1000L);
        AtomicReference<Types.ConsumerGroupList> lastResp = new AtomicReference<>();
        breaker.executeWithFallback(promise -> {
            client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                    .compose(req -> req.send().compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                        Types.ConsumerGroupList response = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class);
                        Types.ConsumerGroupDescription g = response.getItems().stream().filter(i -> i.getGroupId().equals("test-group")).collect(Collectors.toList()).stream().findFirst().get();
                        if (g.getConsumers().size() != 0) {
                            lastResp.set(response);
                            promise.complete();
                        }
                    })));
        }, t -> null);

        try {
            await().atMost(1, TimeUnit.MINUTES).untilAtomic(lastResp, is(notNullValue()));
        } catch (Exception e) {
            testContext.failNow("Test wait for rebalance");
        }

        AtomicInteger parts = new AtomicInteger(0);

        testContext.verify(() -> {
            lastResp.get().getItems().forEach(item -> {
                if (item.getGroupId().equals("test-group")) {
                    for (Types.Consumer c : item.getConsumers()) {
                        parts.getAndIncrement();
                        assertThat(c.getMemberId()).isNull();
                        int actOffset = records.get().records().records(new TopicPartition(topic, c.getPartition())).size();
                        assertThat(c.getOffset()).isEqualTo(actOffset);
                        assertThat(c.getLag()).isNotNegative();
                        assertThat(c.getLag()).isEqualTo(c.getLogEndOffset() - c.getOffset());
                    }
                } else {
                    item.getConsumers().forEach(c -> assertThat(c.getMemberId()).isNull());
                }
            });
            assertThat(parts.get()).isEqualTo(3);
            List<String> responseGroupIDs = lastResp.get().getItems().stream().map(Types.ConsumerGroup::getGroupId).collect(Collectors.toList());
            List<String> consumerGroups = kafkaClient.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
            assertThat(consumerGroups).hasSameElementsAs(responseGroupIDs);
        });

        testContext.completeNow();

        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testListConsumerGroupsWithSortDesc(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> grpIds = SyncMessaging.createConsumerGroups(kafkaClient, 5, deployments.getExternalBootstrapServers(), testContext);
        grpIds.sort(String::compareToIgnoreCase);
        Collections.reverse(grpIds);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups?orderKey=name&order=desc")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.ConsumerGroupList response = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class);
                    List<String> responseGroupIDs = response.getItems().stream().map(Types.ConsumerGroup::getGroupId).collect(Collectors.toList());
                    assertThat(responseGroupIDs).isEqualTo(grpIds);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testListConsumerGroupsWithSortAsc(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> grpIds = SyncMessaging.createConsumerGroups(kafkaClient, 5, deployments.getExternalBootstrapServers(), testContext);

        grpIds.sort(String::compareToIgnoreCase);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups?orderKey=name&order=asc")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.ConsumerGroupList response = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class);
                    List<String> responseGroupIDs = response.getItems().stream().map(Types.ConsumerGroup::getGroupId).collect(Collectors.toList());
                    assertThat(responseGroupIDs).isEqualTo(grpIds);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testListConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        HttpClient client = createHttpClient(vertx);
        deployments.stopKafkaContainer();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups")
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDeleteConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> groupdIds = SyncMessaging.createConsumerGroups(kafkaClient, 5, deployments.getExternalBootstrapServers(), testContext);

        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupdIds.get(0))
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.GROUP_DELETED.code) {
                        testContext.failNow("Status code not correct. Got: " + response.statusCode() + "expected: " + ReturnCodes.GROUP_DELETED.code);
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    groupdIds.remove(0);
                    List<String> consumerGroups = kafkaClient.listConsumerGroups().all().get().stream().map(x -> x.groupId()).collect(Collectors.toList());
                    assertThat(consumerGroups).hasSameElementsAs(groupdIds);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDeleteActiveConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        String groupID = UUID.randomUUID().toString();
        String topicName = UUID.randomUUID().toString();
        KafkaConsumer<String, String> consumer = AsyncMessaging.createActiveConsumerGroup(vertx, kafkaClient,
                deployments.getExternalBootstrapServers(), groupID, topicName);
        AsyncMessaging.consumeMessages(vertx, consumer, topicName, 200);
        DynamicWait.waitForGroupExists(groupID, kafkaClient);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.GROUP_LOCKED.code) {
                        testContext.failNow("Status code not correct got: " + response.statusCode() + " expected: " + ReturnCodes.GROUP_LOCKED.code);
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    List<String> consumerGroups = kafkaClient.listConsumerGroups().all().get()
                            .stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    assertThat(consumerGroups).contains(groupID);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
        consumer.close();
    }

    @Test
    void testDeleteConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> groupdIds = SyncMessaging.createConsumerGroups(kafkaClient, 2, deployments.getExternalBootstrapServers(), testContext);

        HttpClient client = createHttpClient(vertx);
        deployments.stopKafkaContainer();
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupdIds.get(0))
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> groupdIds = SyncMessaging.createConsumerGroups(kafkaClient, 2, deployments.getExternalBootstrapServers(), testContext);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupdIds.get(0))
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct. Got: " + response.statusCode() + " expected: " + ReturnCodes.SUCCESS.code);
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    ConsumerGroupDescription description = kafkaClient.describeConsumerGroups(Collections.singletonList(groupdIds.get(0))).describedGroups().get(groupdIds.get(0)).get();
                    Types.ConsumerGroupDescription cG = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupDescription.class);
                    Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> assignedPartitions = kafkaClient.listConsumerGroupOffsets(groupdIds.get(0)).partitionsToOffsetAndMetadata().get();
                    assertThat(cG.getConsumers().size()).isEqualTo(assignedPartitions.size());
                    assertThat(cG.getState()).isEqualTo(description.state().name());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        List<String> groupdIds = SyncMessaging.createConsumerGroups(kafkaClient, 2, deployments.getExternalBootstrapServers(), testContext);

        HttpClient client = createHttpClient(vertx);
        deployments.stopKafkaContainer();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupdIds.get(0))
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKA_DOWN.code);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeNonExistingConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups/test-1")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() ==  ReturnCodes.NOT_FOUND.code) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        "-1,      p1-w,   1",
        "topic,   GrOuP,  4",
        "opic,    RoUp,   4",
        "topic-1, z,      0",
        ",        grouP2, 2"
    })
    void testConsumerGroupsListFilteredWithoutAuthentication(String topicFilter, String groupFilter, int expectedCount, Vertx vertx, VertxTestContext testContext) throws Exception {
        SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, "topic-1", "group1-W");
        SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, "topic-1", "group1-X");
        SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, "topic-2", "group2-Y");
        SyncMessaging.createConsumerGroups(kafkaClient, 1, externalBootstrap, testContext, "topic-2", "group2-Z");

        List<String> filters = new ArrayList<>(2);

        if (topicFilter != null && !topicFilter.isBlank()) {
            filters.add("topic=" + topicFilter);
        }

        if (groupFilter != null && !groupFilter.isBlank()) {
            filters.add("group-id-filter=" + groupFilter);
        }

        Checkpoint statusVerified = testContext.checkpoint();
        Checkpoint responseBodyVerified = testContext.checkpoint();

        createHttpClient(vertx).request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/consumer-groups?" + String.join("&", filters))
                .compose(req -> req.send())
                .map(resp -> {
                    if (resp.statusCode() != ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                    statusVerified.flag();
                    return resp;
                })
            .compose(HttpClientResponse::body)
            .map(buffer -> MODEL_DESERIALIZER.deserializeResponse(buffer, Types.ConsumerGroupList.class))
            .map(groups -> testContext.verify(() -> {
                assertThat(groups.getItems().size()).isEqualTo(expectedCount);
                groups.getItems()
                    .stream()
                    .map(Types.ConsumerGroupDescription::getConsumers)
                    .flatMap(List::stream)
                    .forEach(consumer -> {
                        if (topicFilter != null && !topicFilter.isBlank()) {
                            assertThat(consumer.getTopic().toLowerCase(Locale.ROOT)).contains(topicFilter.toLowerCase(Locale.ROOT));
                        }
                        if (groupFilter != null && !groupFilter.isBlank()) {
                            assertThat(consumer.getGroupId().toLowerCase(Locale.ROOT)).contains(groupFilter.toLowerCase(Locale.ROOT));
                        }
                    });

                responseBodyVerified.flag();
            }))
            .onFailure(testContext::failNow);
    }
}
