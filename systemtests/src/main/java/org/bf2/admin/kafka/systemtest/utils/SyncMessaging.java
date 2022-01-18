package org.bf2.admin.kafka.systemtest.utils;

import io.vertx.core.Promise;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SyncMessaging {
    protected static final Logger LOGGER = LogManager.getLogger(SyncMessaging.class);

    public static List<String> createConsumerGroups(AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext, String token, String topicPrefix, String groupPrefix) throws Exception {
        Promise<List<String>> promise = Promise.promise();
        List<String> groupIds = Collections.synchronizedList(new ArrayList<>());

        List<NewTopic> newTopics = IntStream.range(0, count)
            .mapToObj(index -> {
                String topicName = topicPrefix + '-' + UUID.randomUUID().toString();
                return new NewTopic(topicName, 1, (short) 1);
            })
            .collect(Collectors.toList());

        kafkaClient.createTopics(newTopics)
            .all()
            .whenComplete((nothing, error) -> {
                if (error != null) {
                    testContext.failNow(error);
                } else {
                    newTopics.stream()
                        .parallel()
                        .forEach(topic -> {
                            String groupName = groupPrefix + '-' + UUID.randomUUID().toString();
                            groupIds.add(groupName);

                            Properties props;

                            if (token != null) {
                                props = ClientsConfig.getConsumerConfigOauth(bootstrap, groupName, token);
                            } else {
                                props = ClientsConfig.getConsumerConfig(bootstrap, groupName);
                            }

                            try (var consumer = new KafkaConsumer<>(props)) {
                                consumer.subscribe(List.of(topic.name()));
                                consumer.poll(Duration.ofSeconds(4));
                            }

                            LOGGER.info("Created group {} on topic {}", groupName, topic.name());
                        });
                }
            }).whenComplete((nothing, error) -> {
                if (error != null) {
                    promise.fail(error);
                } else {
                    promise.complete(groupIds);
                }
            });

        return promise.future()
                .toCompletionStage()
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
    }

    public static List<String> createConsumerGroups(AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext, String token) throws Exception {
        return createConsumerGroups(kafkaClient, count, bootstrap, testContext, token, "topic", "group");
    }

    public static List<String> createConsumerGroups(AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext, String topicPrefix, String groupPrefix) throws Exception {
        return createConsumerGroups(kafkaClient, count, bootstrap, testContext, null, topicPrefix, groupPrefix);
    }

    public static List<String> createConsumerGroups(AdminClient kafkaClient, int count, String bootstrap, VertxTestContext testContext) throws Exception {
        return createConsumerGroups(kafkaClient, count, bootstrap, testContext, null, "topic", "group");
    }

}
