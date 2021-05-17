package org.bf2.admin.kafka.systemtest.plain;

import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.annotations.ParallelTest;
import org.bf2.admin.kafka.systemtest.bases.PlainTestBase;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.bf2.admin.kafka.systemtest.utils.DynamicWait;
import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

public class RestEndpointInternalIT extends PlainTestBase {

    @ParallelTest
    void testTopicListAfterCreationWithInternalTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        List<NewTopic> topics = new ArrayList<>();
        for (int i = 0; i < 2; i++) topics.add(new NewTopic(UUID.randomUUID().toString(), 1, (short) 1));
        topics.add(new NewTopic("__" + UUID.randomUUID().toString(), 1, (short) 1));
        kafkaClient.createTopics(topics);
        DynamicWait.waitForTopicsExists(topics.stream().map(NewTopic::name).collect(Collectors.toList()), kafkaClient);
        HttpClient client = createHttpClient(vertx);
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).hasSameElementsAs(actualRestNames);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testCreateInternalTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws InterruptedException {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        Types.NewTopic topic = RequestUtils.getTopicObject(3);
        topic.setName("__" + topic.getName());

        createHttpClient(vertx).request(HttpMethod.POST, publishedAdminPort, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.TOPIC_CREATED.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(kafkaClient.listTopics().names().get()).contains(topic.getName());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testDescribeInternalTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        final String topicName = "__" + UUID.randomUUID().toString();
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
        createHttpClient(vertx).request(HttpMethod.GET, publishedAdminPort, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.Topic topic = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.Topic.class);
                    assertThat(topic.getPartitions().size()).isEqualTo(2);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testTopicDeleteInternal(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        final String topicName = "__" + UUID.randomUUID().toString();
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        createHttpClient(vertx).request(HttpMethod.DELETE, publishedAdminPort, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParallelTest
    void testUpdateInternalTopic(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        final String topicName = "__" + UUID.randomUUID().toString();
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        createHttpClient(vertx).request(HttpMethod.PATCH, publishedAdminPort, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    DynamicWait.waitForTopicExists(topicName, kafkaClient);
                    ConfigResource resource = new ConfigResource(TOPIC, topicName);
                    String configVal = kafkaClient.describeConfigs(Collections.singletonList(resource))
                            .all().get().get(resource).get("min.insync.replicas").value();
                    assertThat(configVal).isEqualTo("2");
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}
