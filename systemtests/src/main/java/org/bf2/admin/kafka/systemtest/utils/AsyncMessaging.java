package org.bf2.admin.kafka.systemtest.utils;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        LOGGER.info("reset topic {} offset for all partitions to the end", topic);

        return consumer.partitionsFor(topic)
            .map(partitions -> partitions.stream()
                 .map(p -> new TopicPartition(p.getTopic(), p.getPartition()))
                 .collect(Collectors.toSet()))
            .compose(partitions -> {
                LOGGER.info("Assigning partitions to consumer: {}", partitions);
                return consumer.assign(partitions).map(partitions);
            })
            .compose(partitions -> {
                LOGGER.info("Assignment complete, seeking to beginning of partitions: {}", partitions);
                return consumer.seekToBeginning(partitions).map(partitions);
            })
            .compose(partitions ->
                CompositeFuture.all(partitions.stream()
                    .map(consumer::position)
                    .collect(Collectors.toList())))
            .map(positionResults -> {
                positionResults.<Long>list().forEach(p -> {
                    LOGGER.info("Partition to offset {}", p);
                });
                return null;
            })
            .compose(nothing -> {
                LOGGER.info("reset topic {}, commit consumer", topic);
                return consumer.commit();
            })
            .compose(nothing -> {
                LOGGER.info("reset topic {}, unsubscribe consumer", topic);
                return consumer.unsubscribe();
            });
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
        return receiveAsync(consumer, topic, count)
                .compose(consumeFuture -> {
                    var timeoutPromise = Promise.promise();
                    var timeoutTimer = vertx.setTimer(120_000, __ -> {
                        timeoutPromise.fail("timeout after waiting for messages; host:; topic:");
                    });
                    return consumeFuture.onComplete(__ -> {
                        vertx.cancelTimer(timeoutTimer);
                        timeoutPromise.tryComplete();
                    });
                });
    }

    public static KafkaConsumer<String, String> createActiveConsumerGroup(Vertx vertx, AdminClient kafkaClient, String bootstrap, String groupID, String topicName) throws Exception {
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfig(bootstrap, groupID));
        return consumer;
    }

    public static KafkaConsumer<String, String> createConsumer(Vertx vertx, String topic, String groupID, String boostrap) {
        final Properties props = ClientsConfig.getConsumerConfig(boostrap, groupID);

        // Create the consumer using props.
        final KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
        consumer.subscribe(topic);

        return consumer;
    }

    public static void produceMessages(Vertx vertx, String bootstrap, String topicName, int numberOfMessages, String token) {
        try {
            produceMessages(vertx, bootstrap, topicName, numberOfMessages, token, "X")
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static Future<Void> produceMessages(Vertx vertx, String bootstrap, String topicName, int numberOfMessages, String token, String messagePrefix) {
        final Properties props;

        if (token == null) {
            props = ClientsConfig.getProducerConfig(bootstrap);
        } else {
            props = ClientsConfig.getProducerConfigOauth(bootstrap, token);
        }

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, props);
        RandomStringGenerator randomStringGenerator =
                new RandomStringGenerator.Builder()
                        .withinRange('0', 'z')
                        .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
                        .build();

        return vertx.executeBlocking(promise -> {
            @SuppressWarnings("rawtypes")
            List<Future> pendingMessages = IntStream.range(0, numberOfMessages)
                .mapToObj(index -> messagePrefix + "_message_" + randomStringGenerator.generate(100))
                .map(message -> KafkaProducerRecord.<String, String>create(topicName, message))
                .map(producerRecord -> {
                    return producer.send(producerRecord)
                            .onSuccess(meta -> {
                                LOGGER.info("Message " + producerRecord.value() + " written on topic=" + meta.getTopic() +
                                    ", partition=" + meta.getPartition() +
                                    ", offset=" + meta.getOffset());
                            });
                })
                .collect(Collectors.toList());

            CompositeFuture.all(pendingMessages)
                .onFailure(promise::fail)
                .onSuccess(results -> promise.complete());
        });

    }
}
