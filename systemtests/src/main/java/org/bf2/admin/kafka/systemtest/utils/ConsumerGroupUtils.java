package org.bf2.admin.kafka.systemtest.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupUtils {

    final String bootstrapServers;
    final String token;

    public ConsumerGroupUtils(String bootstrapServers, String token) {
        super();
        this.bootstrapServers = bootstrapServers;
        this.token = token;
    }

    public Consumer<String, String> createConsumerGroup(String groupId, String topicName, String consumerClientId, int numPartitions, boolean closeConsumer) {
        Properties adminConfig = token != null ?
            ClientsConfig.getAdminConfigOauth(token, bootstrapServers) :
            ClientsConfig.getAdminConfig(bootstrapServers);

        Properties producerConfig = token != null ?
            ClientsConfig.getProducerConfigOauth(bootstrapServers, token) :
            ClientsConfig.getProducerConfig(bootstrapServers);

        Properties consumerConfig = token != null ?
            ClientsConfig.getConsumerConfigOauth(bootstrapServers, groupId, token) :
            ClientsConfig.getConsumerConfig(bootstrapServers, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, consumerClientId);

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);

        try (Admin admin = Admin.create(adminConfig)) {
            admin.createTopics(List.of(new NewTopic(topicName, numPartitions, (short) 1)))
                .all()
                .toCompletionStage()
                .thenRun(() -> {
                    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                        producer.send(new ProducerRecord<>(topicName, "v")).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenRun(() -> {
                    try {
                        consumer.subscribe(List.of(topicName));
                        var records = consumer.poll(Duration.ofSeconds(1));
                        assertEquals(1, records.count());
                        consumer.commitSync();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toCompletableFuture()
                .get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            consumer.close();
            throw new RuntimeException(e);
        }

        if (closeConsumer) {
            consumer.close();
            return null;
        }

        return consumer;
    }

}
