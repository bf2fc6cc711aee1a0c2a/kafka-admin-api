package org.bf2.admin.kafka.systemtest.oauth;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.NewTopic;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.bases.OauthTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.json.OffsetModel;
import org.bf2.admin.kafka.systemtest.json.PartitionsModel;
import org.bf2.admin.kafka.systemtest.utils.AsyncMessaging;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Java6Assertions.assertThat;


public class PartitionsOffsetOauthIT extends OauthTestBase {

    @Test
    void testResetOffsetToStartAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));

        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 10, token);
        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();
        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), new ArrayList<>()));

        OffsetModel model = new OffsetModel("relative", "earliest", partList);
        CountDownLatch cd2 = new CountDownLatch(1);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityEnabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.TopicPartitionResetResult result = MODEL_DESERIALIZER.getResetResult(buffer);
                    assertThat(result.getOffset()).isEqualTo(0);
                    assertThat(result.getTopic()).isEqualTo(topic.name());
                    cd2.countDown();
                })));

        assertThat(cd2.await(1, TimeUnit.MINUTES)).isTrue();

        KafkaConsumer<String, String> consumer2 = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        consumer2.subscribe(topic.name());
        consumer2.poll(Duration.ofSeconds(20), result -> testContext.verify(() -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.result().size()).isEqualTo(10);
        }));
        consumer2.close();
        testContext.completeNow();
    }

    @Test
    void testResetOffsetUnauthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));

        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 10, token);
        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();
        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), new ArrayList<>()));
        OffsetModel model = new OffsetModel("relative", "latest", partList);
        changeTokenToUnauthorized(vertx, testContext);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            assertStrictTransportSecurityEnabled(response, testContext);
                            testContext.completeNow();
                        })).onFailure(testContext::failNow));
        testContext.awaitCompletion(1, TimeUnit.MINUTES);
        testContext.completeNow();
    }

    @Test
    void testResetOffsetToEndAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));

        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 10, token);
        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();
        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), new ArrayList<>()));

        OffsetModel model = new OffsetModel("relative", "latest", partList);
        CountDownLatch cd2 = new CountDownLatch(1);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityEnabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.TopicPartitionResetResult result = MODEL_DESERIALIZER.getResetResult(buffer);
                    assertThat(result).isNotEqualTo(null);
                    assertThat(result.getOffset()).isEqualTo(10);
                    assertThat(result.getTopic()).isEqualTo(topic.name());
                    cd2.countDown();
                })));
        assertThat(cd2.await(1, TimeUnit.MINUTES)).isTrue();


        KafkaConsumer<String, String> consumer2 = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        consumer2.subscribe(topic.name());
        consumer2.poll(Duration.ofSeconds(20), result -> testContext.verify(() -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.result().size()).isEqualTo(0);
        }));
        consumer2.close();
        testContext.completeNow();
    }

    @Test
    void testResetOffsetToTargetAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));

        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 10, token);
        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();
        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), new ArrayList<>()));

        OffsetModel model = new OffsetModel("absolute", "5", partList);
        CountDownLatch cd2 = new CountDownLatch(1);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityEnabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.TopicPartitionResetResult result = MODEL_DESERIALIZER.getResetResult(buffer);
                    assertThat(result).isNotEqualTo(null);
                    assertThat(result.getOffset()).isEqualTo(5);
                    assertThat(result.getTopic()).isEqualTo(topic.name());
                    cd2.countDown();
                })));
        assertThat(cd2.await(1, TimeUnit.MINUTES)).isTrue();


        KafkaConsumer<String, String> consumer2 = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        consumer2.subscribe(topic.name());
        consumer2.poll(Duration.ofSeconds(20), result -> testContext.verify(() -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.result().size()).isEqualTo(5);
        }));
        consumer2.close();
        testContext.completeNow();
    }

    //@Disabled
    @Test
    void testResetOffsetToTimestampAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 1, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));

        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 5, token, "A");
        // Sleep between sections
        Thread.sleep(10_000);

        DateTimeFormatter sdfDate = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'" + ZonedDateTime.now().getZone().getId() + "'");
        String timestamp = sdfDate.format(LocalDateTime.now());

        Thread.sleep(10_000);
        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 5, token, "B");

        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));
        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();

        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), new ArrayList<>()));

        OffsetModel model = new OffsetModel("timestamp", timestamp, partList);
        CountDownLatch cd2 = new CountDownLatch(1);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityEnabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.TopicPartitionResetResult result = MODEL_DESERIALIZER.getResetResult(buffer);
                    assertThat(result).isNotEqualTo(null);
                    assertThat(result.getOffset()).isEqualTo(5);
                    assertThat(result.getTopic()).isEqualTo(topic.name());
                    cd2.countDown();
                })));
        assertThat(cd2.await(1, TimeUnit.MINUTES)).isTrue();


        KafkaConsumer<String, String> consumer2 = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        consumer2.subscribe(topic.name());
        consumer2.poll(Duration.ofSeconds(20), result -> testContext.verify(() -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.result().size()).isEqualTo(5);
            result.result().records().records(topic.name()).forEach(record -> {
                assertThat(record.value()).containsSequence("B");
            });
        }));
        consumer2.close();
        testContext.completeNow();
    }

    @Test
    void testResetOffsetOnMultiplePartitionsAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        NewTopic topic = new NewTopic(UUID.randomUUID().toString(), 3, (short) 1);
        String groupID = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(topic));
        CountDownLatch cd = new CountDownLatch(1);
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));

        AsyncMessaging.consumeMessages(vertx, consumer, topic.name(), 10).onComplete(x -> cd.countDown()).onFailure(y -> testContext.failNow("Could not receive messages"));
        AsyncMessaging.produceMessages(vertx, "localhost:9092", topic.name(), 10, token);

        assertThat(cd.await(2, TimeUnit.MINUTES)).isTrue();
        consumer.close();

        List<PartitionsModel> partList = Collections.singletonList(new PartitionsModel(topic.name(), Collections.singletonList(0)));

        OffsetModel model = new OffsetModel("absolute", "5", partList);
        CountDownLatch cd2 = new CountDownLatch(1);
        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/consumer-groups/" + groupID + "/reset-offset")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(model)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            assertStrictTransportSecurityEnabled(response, testContext);
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.TopicPartitionResetResult result = MODEL_DESERIALIZER.getResetResult(buffer);
                    assertThat(result).isNotEqualTo(null);
                    assertThat(result.getOffset()).isEqualTo(5);
                    assertThat(result.getTopic()).isEqualTo(topic.name());
                    cd2.countDown();
                })));
        assertThat(cd2.await(1, TimeUnit.MINUTES)).isTrue();

        KafkaConsumer<String, String> consumer2 = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth("localhost:9092", groupID, token));
        TopicPartition partition = new TopicPartition(topic.name(), 0);
        consumer2.assign(partition);
        CountDownLatch cd3 = new CountDownLatch(1);
        consumer2.poll(Duration.ofSeconds(20), result -> testContext.verify(() -> {
            assertThat(result.succeeded()).isTrue();
            assertThat(result.result().size()).isEqualTo(5);
            result.result().records().records(topic.name()).forEach(record -> {
                assertThat(record.partition()).isEqualTo(0);
            });
            cd3.countDown();
        }));
        assertThat(cd3.await(1, TimeUnit.MINUTES)).isTrue();
        consumer2.close();
        testContext.completeNow();
    }
}
