package org.bf2.admin.kafka.systemtest.plain;

import org.bf2.admin.kafka.systemtest.utils.RequestUtils;
import org.bf2.admin.kafka.systemtest.annotations.ParallelTest;
import org.bf2.admin.kafka.systemtest.bases.PlainTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricsEndpointTestIT extends PlainTestBase {

    @ParallelTest
    void testAdminListMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) {
        HttpClient client = createHttpsClient(vertx);
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        RequestUtils.prepareAndExecuteListRequest(testContext, 3, client, publishedAdminPort);

        String metrics = RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^list_topics_requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("3.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminCreateMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) {
        HttpClient client = createHttpsClient(vertx);
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        RequestUtils.prepareAndExecuteCreateRequest(testContext, 4, client, publishedAdminPort);
        String metrics = RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^create_topic_requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("4.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminDeleteMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        HttpClient client = createHttpsClient(vertx);
        RequestUtils.prepareAndExecuteDeleteRequest(testContext, 2, client, kafkaClient, publishedAdminPort);
        String metrics =  RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^delete_topic_requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("2.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        kafkaClient.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminDescribeMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        HttpClient client = createHttpsClient(vertx);
        RequestUtils.prepareAndExecuteDescribeRequest(testContext, 3, client, kafkaClient, publishedAdminPort);
        String metrics =  RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^describe_topic_requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("3.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        kafkaClient.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminUpdateMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        HttpClient client = createHttpsClient(vertx);
        RequestUtils.prepareAndExecuteUpdateRequest(testContext, 2, client, kafkaClient, publishedAdminPort);
        String metrics =  RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^update_topic_requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("2.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        kafkaClient.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminTotalMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        HttpClient client = createHttpsClient(vertx);
        RequestUtils.prepareAndExecuteUpdateRequest(testContext, 2, client, kafkaClient, publishedAdminPort);
        RequestUtils.prepareAndExecuteDeleteRequest(testContext, 3, client, kafkaClient, publishedAdminPort);
        RequestUtils.prepareAndExecuteDescribeRequest(testContext, 1, client, kafkaClient, publishedAdminPort);
        RequestUtils.prepareAndExecuteCreateRequest(testContext, 4, client, publishedAdminPort);
        RequestUtils.prepareAndExecuteListRequest(testContext, 6, client, publishedAdminPort);
        String metrics =  RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern pattern = Pattern.compile("^requests_total ([0-9.]+)", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(metrics);
        if (matcher.find()) {
            assertThat(matcher.group(1)).isEqualTo("16.0");
        } else {
            testContext.failNow("Could not find correct metric");
        }
        client.close();
        kafkaClient.close();
        testContext.completeNow();
    }

    @ParallelTest
    void testAdminSucceededAndFailedMetrics(Vertx vertx, VertxTestContext testContext, ExtensionContext extensionContext) throws Exception {
        AdminClient kafkaClient = AdminClient.create(RequestUtils.getKafkaAdminConfig(DEPLOYMENT_MANAGER
                .getKafkaContainer(extensionContext).getBootstrapServers()));
        int publishedAdminPort = DEPLOYMENT_MANAGER.getAdminPort(extensionContext);
        HttpClient client = createHttpsClient(vertx);
        RequestUtils.prepareAndExecuteFailDeleteRequest(testContext, 5, client, publishedAdminPort);
        RequestUtils.prepareAndExecuteDeleteRequest(testContext, 3, client, kafkaClient, publishedAdminPort);
        RequestUtils.prepareAndExecuteListRequest(testContext, 2, client, publishedAdminPort);
        RequestUtils.prepareAndExecuteFailCreateTopicRequest(testContext, 4, client, publishedAdminPort);

        String metrics =  RequestUtils.retrieveMetrics(vertx, extensionContext, testContext);
        Pattern patternTotal = Pattern.compile("^requests_total ([0-9.]+)", Pattern.MULTILINE);
        Pattern patternFailedNotFound = Pattern.compile("^failed_requests_total\\{status_code=\"404\",\\} ([0-9.]+)", Pattern.MULTILINE);
        Pattern patternFailedBadRequest = Pattern.compile("^failed_requests_total\\{status_code=\"400\",\\} ([0-9.]+)", Pattern.MULTILINE);
        Pattern patternSucc = Pattern.compile("^succeeded_requests_total ([0-9.]+)", Pattern.MULTILINE);
        HashMap<Matcher, String> matchers = new HashMap<Matcher, String>() {{
                put(patternTotal.matcher(metrics), "14.0");
                put(patternFailedNotFound.matcher(metrics), "5.0");
                put(patternFailedBadRequest.matcher(metrics), "4.0");
                put(patternSucc.matcher(metrics), "5.0");
            }};

        matchers.forEach((matcher, expected) -> testContext.verify(() -> {
            if (matcher.find()) {
                assertThat(matcher.group(1)).isEqualTo(expected);
            } else {
                testContext.failNow("Could not find correct metric");
            }
        }));
        client.close();
        kafkaClient.close();
        testContext.completeNow();
    }
}
