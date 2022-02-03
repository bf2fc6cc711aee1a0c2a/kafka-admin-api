package org.bf2.admin.kafka.systemtest.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ConsumerUtils {

    final String bootstrapServers;
    final String token;

    public ConsumerUtils(String bootstrapServers, String token) {
        super();
        this.bootstrapServers = bootstrapServers;
        this.token = token;
    }

    public ConsumerRequest request() {
        return new ConsumerRequest();
    }

    public class ConsumerRequest {
        String groupId;
        String clientId;
        String topicName;
        boolean createTopic = true;
        int numPartitions = 1;
        int produceMessages = 0;
        int consumeMessages = 0;
        boolean autoClose = false;

        public ConsumerRequest topic(String topicName, int numPartitions) {
            this.topicName = topicName;
            this.numPartitions = numPartitions;
            return this;
        }

        public ConsumerRequest topic(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public ConsumerRequest createTopic(boolean createTopic) {
            this.createTopic = createTopic;
            return this;
        }

        public ConsumerRequest groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public ConsumerRequest clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ConsumerRequest produceMessages(int produceMessages) {
            this.produceMessages = produceMessages;
            return this;
        }

        public ConsumerRequest consumeMessages(int consumeMessages) {
            this.consumeMessages = consumeMessages;
            return this;
        }

        public ConsumerRequest autoClose(boolean autoClose) {
            this.autoClose = autoClose;
            return this;
        }

        public ConsumerResponse consume() {
            return ConsumerUtils.this.consume(this, autoClose);
        }
    }

    public class ConsumerResponse implements Closeable {
        Consumer<String, String> consumer;
        ConsumerRecords<String, String> records;

        @Override
        public void close() {
            if (consumer != null) {
                consumer.close();
            }
        }

        public Consumer<String, String> consumer() {
            return consumer;
        }

        public ConsumerRecords<String, String> records() {
            return records;
        }
    }

    @SuppressWarnings("resource")
    public Consumer<String, String> consume(String groupId, String topicName, String clientId, int numPartitions, boolean autoClose) {
        return request().groupId(groupId).topic(topicName, numPartitions).clientId(clientId).autoClose(autoClose).consume().consumer;
    }

    ConsumerResponse consume(ConsumerRequest consumerRequest, boolean autoClose) {
        Properties adminConfig = token != null ?
            ClientsConfig.getAdminConfigOauth(token, bootstrapServers) :
            ClientsConfig.getAdminConfig(bootstrapServers);

        Properties producerConfig = token != null ?
            ClientsConfig.getProducerConfigOauth(bootstrapServers, token) :
            ClientsConfig.getProducerConfig(bootstrapServers);

        Properties consumerConfig = token != null ?
            ClientsConfig.getConsumerConfigOauth(bootstrapServers, consumerRequest.groupId, token) :
            ClientsConfig.getConsumerConfig(bootstrapServers, consumerRequest.groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, consumerRequest.clientId);
        if (consumerRequest.consumeMessages > 0) {
            consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(consumerRequest.consumeMessages));
        }

        ConsumerResponse response = new ConsumerResponse();
        response.consumer = new KafkaConsumer<>(consumerConfig);

        try (Admin admin = Admin.create(adminConfig)) {
            CompletionStage<Void> initial;

            if (consumerRequest.createTopic) {
                initial = admin.createTopics(List.of(new NewTopic(consumerRequest.topicName, consumerRequest.numPartitions, (short) 1)))
                    .all()
                    .toCompletionStage();
            } else {
                initial = CompletableFuture.completedStage(null);
            }

            initial.thenRun(() -> {
                    if (consumerRequest.produceMessages < 1) {
                        return;
                    }

                    try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                        for (int i = 0; i < consumerRequest.produceMessages; i++) {
                            producer.send(new ProducerRecord<>(consumerRequest.topicName, "message-" + i)).get();
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenRun(() -> {
                    try {
                        response.consumer.subscribe(List.of(consumerRequest.topicName));
                        response.records = response.consumer.poll(Duration.ofSeconds(10));
                        response.consumer.commitSync();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toCompletableFuture()
                .get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            response.consumer.close();
            throw new RuntimeException(e);
        }

        if (autoClose) {
            response.consumer.close();
        }

        return response;
    }

}
