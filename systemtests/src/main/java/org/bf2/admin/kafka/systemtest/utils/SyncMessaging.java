package org.bf2.admin.kafka.systemtest.utils;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.bf2.admin.kafka.systemtest.json.TokenModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SyncMessaging {

    public static List<String> createConsumerGroups(Vertx vertx, AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext, TokenModel token) throws Exception {
        List<String> groupIds = new ArrayList<>();
        testContext.verify(() -> {
            for (int i = 0; i < count; i++) {
                String topicName = UUID.randomUUID().toString();
                groupIds.add(UUID.randomUUID().toString());
                kafkaClient.createTopics(Collections.singletonList(
                        new NewTopic(topicName, 1, (short) 1)
                ));
                DynamicWait.waitForTopicExists(topicName, kafkaClient);
                KafkaConsumer<String, String> c = new KafkaConsumer<>(ClientsConfig.getConsumerConfigOauth(bootstrap, groupIds.get(i), token));
                c.subscribe(Collections.singletonList(topicName));
                c.poll(Duration.ofSeconds(5));
                DynamicWait.waitForGroupExists(groupIds.get(i), kafkaClient);
                c.close();
            }
        });
        return groupIds;
    }

    public static List<String> createConsumerGroups(Vertx vertx, AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext) throws Exception {
        List<String> groupIds = new ArrayList<>();
        testContext.verify(() -> {
            for (int i = 0; i < count; i++) {
                String topicName = UUID.randomUUID().toString();
                groupIds.add(UUID.randomUUID().toString());
                kafkaClient.createTopics(Collections.singletonList(
                        new NewTopic(topicName, 1, (short) 1)
                ));
                DynamicWait.waitForTopicExists(topicName, kafkaClient);
                KafkaConsumer<String, String> c = new KafkaConsumer<>(ClientsConfig.getConsumerConfig(bootstrap, groupIds.get(i)));
                c.subscribe(Collections.singletonList(topicName));
                c.poll(Duration.ofSeconds(5));
                DynamicWait.waitForGroupExists(groupIds.get(i), kafkaClient);
                c.close();
            }
        });
        return groupIds;
    }

    public static KafkaProducer<String, String> createProducer(String bootstrap) {
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ClientsConfig.getProducerConfig(bootstrap));
        return producer;
    }
}
