package org.bf2.admin.kafka.systemtest.plain;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.annotations.ParallelTest;
import org.bf2.admin.kafka.systemtest.bases.PlainTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.AsyncMessaging;
import org.bf2.admin.kafka.systemtest.utils.ClientsConfig;
import org.bf2.admin.kafka.systemtest.utils.DynamicWait;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.bf2.admin.kafka.systemtest.utils.SyncMessaging;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupsEndpointTestIT extends PlainTestBase {

    @ParallelTest
    void testListConsumerGroups(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        SyncMessaging.createConsumerGroups(vertx, kafkaClient, 5, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);
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

    @ParallelTest
    void testEmptyTopicsOnList(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        SyncMessaging.createConsumerGroups(vertx, kafkaClient, 4, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);
        String topic = UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)));
        DynamicWait.waitForTopicsExists(Collections.singletonList(topic), kafkaClient);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, ClientsConfig.getConsumerConfig(DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), "test-grp"));
        AsyncMessaging.consumeMessages(vertx, consumer, topic, 11).onFailure(y -> testContext.failNow("Could not receive messages"));

        AsyncMessaging.produceMessages(vertx, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), topic, 10, null);


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
                    response.getItems().forEach(item -> {
                        if (item.getGroupId().equals("test-grp")) {
                            Types.Consumer c = item.getConsumers().iterator().next();
                            assertThat(c).isNotNull();
                            assertThat(c.getLag()).isEqualTo(10);
                        } else {
                            item.getConsumers().forEach(c -> assertThat(c.getMemberId()).isNull());
                        }
                    });
                    List<String> responseGroupIDs = response.getItems().stream().map(Types.ConsumerGroup::getGroupId).collect(Collectors.toList());
                    assertThat(consumerGroups).hasSameElementsAs(responseGroupIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
        consumer.close();
    }

    @ParallelTest
    void testListConsumerGroupsWithSortDesc(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        List<String> grpIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 5, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);
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
                    assertThat(grpIds).isEqualTo(responseGroupIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testListConsumerGroupsWithSortAsc(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        List<String> grpIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 5, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);

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
                    assertThat(grpIds).isEqualTo(responseGroupIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testListConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        HttpClient client = createHttpClient(vertx);
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getContainerId()).exec();
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
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

    @ParallelTest
    void testDeleteConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        List<String> groupdIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 5, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);

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

    @ParallelTest
    void testDeleteActiveConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        String groupID = UUID.randomUUID().toString();
        String topicName = UUID.randomUUID().toString();
        KafkaConsumer<String, String> consumer = AsyncMessaging.createActiveConsumerGroup(vertx, kafkaClient,
                DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), groupID, topicName);
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

    @ParallelTest
    void testDeleteConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        List<String> groupdIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 2, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);

        HttpClient client = createHttpClient(vertx);
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getContainerId()).exec();
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
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

    @ParallelTest
    void testDescribeConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        List<String> groupdIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 2, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);
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
                    Map<TopicPartition, OffsetAndMetadata> assignedPartitions = kafkaClient.listConsumerGroupOffsets(groupdIds.get(0)).partitionsToOffsetAndMetadata().get();
                    assertThat(cG.getConsumers().size()).isEqualTo(assignedPartitions.size());
                    assertThat(cG.getState()).isEqualTo(description.state().name());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testDescribeConsumerGroupKafkaDown(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        List<String> groupdIds = SyncMessaging.createConsumerGroups(vertx, kafkaClient, 2, DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers(), testContext);

        HttpClient client = createHttpClient(vertx);
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getContainerId()).exec();
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
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

    @ParallelTest
    void testDescribeNonExistingConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

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
}
