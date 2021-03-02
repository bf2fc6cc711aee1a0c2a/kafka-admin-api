package admin.kafka.systemtest.plain;

import admin.kafka.admin.model.Types;
import admin.kafka.systemtest.enums.ReturnCodes;
import admin.kafka.systemtest.utils.RequestUtils;
import admin.kafka.systemtest.bases.PlainTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupsEndpointTestIT extends PlainTestBase {

    @Test
    void testListConsumerGroups(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        String bootstrap = DEPLOYMENT_MANAGER.getKafkaContainer(extensionContext).getBootstrapServers();
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(bootstrap));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        RequestUtils.createConsumerGroups(kafkaClient, 3, bootstrap);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/groups")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    List<String> expectedIDs = kafkaClient.listConsumerGroups().all()
                            .get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    List<String> actualIDs = MODEL_DESERIALIZER.getGroupsId(buffer);
                    assertThat(actualIDs).hasSameClassAs(expectedIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDeleteConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        String bootstrap = DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers();
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(bootstrap));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        RequestUtils.createConsumerGroups(kafkaClient, 3, bootstrap);
        List<String> expectedIDs = kafkaClient.listConsumerGroups().all()
                .get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.DELETE, publishedAdminPort, "localhost", "/rest/groups/" + expectedIDs.get(0))
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    expectedIDs.remove(0);
                    List<String> actualIDs = kafkaClient.listConsumerGroups().all()
                            .get().stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
                    assertThat(actualIDs).isEqualTo(expectedIDs);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeConsumerGroup(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        String bootstrap = DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers();
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(bootstrap));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);

        String groupID = RequestUtils.createConsumerGroup(kafkaClient, bootstrap);
        DescribeConsumerGroupsResult groupsResult = kafkaClient.describeConsumerGroups(Collections.singletonList(groupID));
        ConsumerGroupDescription groupDescAct = groupsResult.all().get().get(groupID);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, publishedAdminPort, "localhost", "/rest/groups/" + groupID)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCCESS.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(testContext.failed()).isFalse();
                    Types.ConsumerGroupDescription groupDescription = MODEL_DESERIALIZER.getGroupDesc(buffer);
                    assertThat(groupDescription.getState().toLowerCase(Locale.ROOT))
                            .isEqualTo(groupDescAct.state().toString().toLowerCase(Locale.ROOT));
                    assertThat(groupDescription.getMembers().size()).isEqualTo(groupDescAct.members().size());
                    assertThat(groupDescription.getSimple()).isEqualTo(groupDescAct.isSimpleConsumerGroup());
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}
