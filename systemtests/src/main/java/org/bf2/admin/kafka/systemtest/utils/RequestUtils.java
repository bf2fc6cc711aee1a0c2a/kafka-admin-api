package org.bf2.admin.kafka.systemtest.utils;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RequestUtils {

    static final Logger LOGGER = LogManager.getLogger(RequestUtils.class);

    public static Types.NewTopic getTopicObject(int partitions) {
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(UUID.randomUUID().toString());
        Types.NewTopicInput topicInput = new Types.NewTopicInput();
        topicInput.setNumPartitions(partitions);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topicInput.setConfig(Collections.singletonList(config));
        topic.setSettings(topicInput);
        return topic;
    }
    public static Types.NewTopic getTopicObject(String name, int partitions) {
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(name);
        Types.NewTopicInput topicInput = new Types.NewTopicInput();
        topicInput.setNumPartitions(partitions);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topicInput.setConfig(Collections.singletonList(config));
        topic.setSettings(topicInput);
        return topic;
    }

    public static void prepareAndExecuteListRequest(VertxTestContext testContext, int count, HttpClient client, int port) {
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            client.request(HttpMethod.GET, port, "localhost", "/rest/topics")
                    .compose(req -> req.send().onSuccess(response -> {
                        if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                            testContext.failNow("Status code not correct");
                        }
                    }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer -> {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    }));
            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not list topics");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static void prepareAndExecuteCreateRequest(VertxTestContext testContext, int count, HttpClient client, int port) {
        Types.NewTopic topic = new Types.NewTopic();
        Types.NewTopicInput topicInput = new Types.NewTopicInput();
        topicInput.setNumPartitions(3);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topicInput.setConfig(Collections.singletonList(config));
        topic.setSettings(topicInput);
        ModelDeserializer deserializer = new ModelDeserializer();
        for (int i = 0; i < count; i++) {
            String topicName = UUID.randomUUID().toString();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            topic.setName(topicName);

            client.request(HttpMethod.POST, port, "localhost", "/rest/topics")
                    .compose(req -> req.putHeader("content-type", "application/json")
                            .send(deserializer.serializeBody(topic)).onSuccess(response -> {
                                if (response.statusCode() !=  ReturnCodes.TOPIC_CREATED.code) {
                                    testContext.failNow("Status code " + response.statusCode() + " is not correct");
                                }
                            }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer -> {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    }));

            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not create topics");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static void prepareAndExecuteDeleteRequest(VertxTestContext testContext, int count, HttpClient client, AdminClient kafkaClient, int port) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            topics.add(new NewTopic(UUID.randomUUID().toString(), 2, (short) 1));
        }
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            client.request(HttpMethod.DELETE, port, "localhost", "/rest/topics/" + topics.get(i).name())
                    .compose(req -> req.putHeader("content-type", "application/json")
                            .send().onSuccess(response -> {
                                if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                    testContext.failNow("Status code " + response.statusCode() + " is not correct");
                                }
                            }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer -> {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    }));

            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not delete topics");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static void prepareAndExecuteFailDeleteRequest(VertxTestContext testContext, int count, HttpClient client, int port) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            topics.add(new NewTopic(UUID.randomUUID().toString(), 2, (short) 1));
        }
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            client.request(HttpMethod.DELETE, port, "localhost", "/rest/topics/" + topics.get(i).name())
                    .compose(req -> req.putHeader("content-type", "application/json")
                            .send().onSuccess(response -> {
                                if (response.statusCode() !=  ReturnCodes.NOT_FOUND.code) {
                                    testContext.failNow("Status code " + response.statusCode() + " is not correct");
                                }
                            }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(buffer -> {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    });

            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not execute fail delete request");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static void prepareAndExecuteFailCreateTopicRequest(VertxTestContext testContext, int count, HttpClient client, int port) throws Exception {
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            Types.NewTopic topic = RequestUtils.getTopicObject(101);

            client.request(HttpMethod.POST, port, "localhost", "/rest/topics")
                  .compose(req -> req.putHeader("content-type", "application/json")
                                     .send(new ModelDeserializer().serializeBody(topic)).onSuccess(response -> {
                                         if (response.statusCode() != ReturnCodes.FAILED_REQUEST.code) {
                                             testContext.failNow("Status code " + response.statusCode() + " is not correct");
                                         }
                                     }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                  .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                      assertThat(testContext.failed()).isFalse();
                      countDownLatch.countDown();
                  })));

            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not execute fail delete request");
                testContext.completeNow();
                e.printStackTrace();
            }
        }
    }

    public static void prepareAndExecuteDescribeRequest(VertxTestContext testContext, int count, HttpClient client, AdminClient kafkaClient, int port) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            topics.add(new NewTopic(UUID.randomUUID().toString(), 2, (short) 1));
        }
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            client.request(HttpMethod.GET, port, "localhost", "/rest/topics/" + topics.get(i).name())
                    .compose(req -> req.send().onSuccess(response -> {
                        if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                            testContext.failNow("Status code not correct");
                        }
                    }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer -> {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    }));

            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not describe topic");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static void prepareAndExecuteUpdateRequest(VertxTestContext testContext, int count, HttpClient client, AdminClient kafkaClient, int port) throws Exception {
        final String configKey = "min.insync.replicas";
        Types.Topic updatedTopic = new Types.Topic();
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        updatedTopic.setConfig(Collections.singletonList(conf));
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            topics.add(new NewTopic(UUID.randomUUID().toString(), 2, (short) 1));
        }
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);
        ModelDeserializer deserializer = new ModelDeserializer();
        for (int i = 0; i < count; i++) {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            updatedTopic.setName(topics.get(i).name());
            client.request(HttpMethod.PATCH, port, "localhost", "/rest/topics/" + topics.get(i).name())
                    .compose(req -> req.putHeader("content-type", "application/json")
                            .send(deserializer.serializeBody(updatedTopic)).onSuccess(response -> {
                                if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                    testContext.failNow("Status code " + response.statusCode() + " is not correct");
                                }
                            }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                    .onComplete(testContext.succeeding(buffer ->  {
                        assertThat(testContext.failed()).isFalse();
                        countDownLatch.countDown();
                    }));
            try {
                countDownLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                testContext.failNow("Could not describe topic");
                testContext.completeNow();
                e.printStackTrace();
            }
        }

    }

    public static String retrieveMetrics(Vertx vertx, ExtensionContext extensionContext, VertxTestContext testContext, int port) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        LOGGER.info("Fetching metrics from {}:{}{}", "localhost", port, "/metrics");

        Future<Buffer> metricsBuffer = vertx.createHttpClient().request(HttpMethod.GET, port, "localhost", "/metrics")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> {
                    assertThat(testContext.failed()).isFalse();
                    countDownLatch.countDown();
                }));
        try {
            countDownLatch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
            testContext.failNow("Could not retrieve metrics.");
            testContext.completeNow();
        }
        return metricsBuffer.result().toString();
    }

    private static Consumer<Long, String> createConsumer(String topic, String groupID, String boostrap) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID == null ? UUID.randomUUID().toString() : groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public static void createConsumerGroups(AdminClient kafkaClient, int count, String bootstrap) throws Exception {
        for (int i = 0; i < count; i++) {
            String topicName = UUID.randomUUID().toString();
            kafkaClient.createTopics(Collections.singletonList(
                    new NewTopic(topicName, 1, (short) 1)
            ));
            DynamicWait.waitForTopicExists(topicName, kafkaClient);
            Consumer<Long, String> consumer = createConsumer(topicName, null, bootstrap);
            consumer.poll(Duration.ofSeconds(1));
            consumer.close();
        }
    }

    public static String createConsumerGroup(AdminClient kafkaClient, String bootstrap) throws Exception {
        String groupID = UUID.randomUUID().toString();
        String topicName = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        Consumer<Long, String> consumer = createConsumer(topicName, groupID, bootstrap);
        consumer.poll(Duration.ofSeconds(1));
        consumer.close();
        return groupID;
    }

    public static Map<String, Object> getKafkaAdminConfig(String bootstrap) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return conf;
    }



}
