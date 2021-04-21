package org.bf2.admin.kafka.systemtest.utils;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.json.TokenModel;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class AsyncMessaging {
    protected static final Logger LOGGER = LogManager.getLogger(AsyncMessaging.class);

    /**
     * Subscribe at the end of the topic
     */
    public static Future<Future<List<KafkaConsumerRecord<String, String>>>> receiveAsync(KafkaConsumer<String, String> consumer, String topicName, int expectedMessages) {

        // start by resetting the topic to the end
        return resetToEnd(consumer, topicName)

                .compose(__ -> {
                    LOGGER.info("subscribe to topic: {}", topicName);
                    return consumer.subscribe(topicName);
                })

                .map(__ -> {
                    LOGGER.info("consumer successfully subscribed to topic: {}", topicName);

                    // set the handler and consume the expected messages
                    return consumeMessages(consumer, expectedMessages);
                });
    }

    /**
     * Subscribe at the end of the topic
     */
    private static Future<Void> resetToEnd(KafkaConsumer<String, String> consumer, String topic) {

        LOGGER.info("rest topic {} offset for all partitions to the end", topic);
        return consumer.partitionsFor(topic)

                // seek the end for all partitions in a topic
                .compose(partitions -> CompositeFuture.all(partitions.stream()
                        .map(partition -> {
                            var tp = new TopicPartition(partition.getTopic(), partition.getPartition());
                            return (Future) consumer.assign(tp)
                                    .compose(__ -> consumer.seekToEnd(tp))

                                    // the seekToEnd take place only once consumer.position() is called
                                    .compose(__ -> consumer.position(tp)
                                            .onSuccess(p -> LOGGER.info("reset partition {}-{} to offset {}", tp.getTopic(), tp.getPartition(), p)));
                        })
                        .collect(Collectors.toList())))

                // commit all partitions offset
                .compose(__ -> consumer.commit())

                // unsubscribe from  all partitions
                .compose(__ -> consumer.unsubscribe());
    }

    private static Future<List<KafkaConsumerRecord<String, String>>> consumeMessages(KafkaConsumer<String, String> consumer, int expectedMessages) {
        Promise<List<KafkaConsumerRecord<String, String>>> promise = Promise.promise();
        List<KafkaConsumerRecord<String, String>> messages = new LinkedList<>();

        // set the fetch batch to the expected messages
        consumer.fetch(expectedMessages);

        consumer.handler(record -> {
            LOGGER.info("Message accepted");
            messages.add(record);
            if (messages.size() == expectedMessages) {
                LOGGER.info("successfully received {} messages", expectedMessages);
                consumer.commit()
                        .compose(__ -> consumer.unsubscribe())
                        .map(__ -> messages).onComplete(promise);
            }
        });

        return promise.future();
    }

    public static Future<List<KafkaConsumerRecord<String, String>>> consumeMessages(Vertx vertx, KafkaConsumer<String, String> consumer, String topic, int count) {
        return receiveAsync(consumer, topic, count).compose(consumeFuture -> {
            var timeoutPromise = Promise.promise();
            var timeoutTimer = vertx.setTimer(120_000, __ -> {
                timeoutPromise.fail("timeout after waiting for messages; host:; topic:");
            });
            var completeFuture = consumeFuture.onComplete(__ -> {
                vertx.cancelTimer(timeoutTimer);
                timeoutPromise.tryComplete();
            });
            return completeFuture;
        });
    }
    public static KafkaConsumer<String, String> createActiveConsumerGroupOauth(Vertx vertx, AdminClient kafkaClient, String bootstrap, String groupID, String topicName, TokenModel token) throws Exception {
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfigOauth(bootstrap, groupID, token));

        return consumer;
    }

    public static KafkaConsumer<String, String> createActiveConsumerGroup(Vertx vertx, AdminClient kafkaClient, String bootstrap, String groupID, String topicName) throws Exception {
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfig(bootstrap, groupID));
        return consumer;
    }

    private static KafkaConsumer<String, String> createConsumer(Vertx vertx, String topic, String groupID, String boostrap) {
        final Properties props = ClientsConfig.getConsumerConfig(boostrap, groupID);


        // Create the consumer using props.
        final KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
        consumer.subscribe(topic);

        return consumer;
    }
}
