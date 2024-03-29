package org.bf2.admin.kafka.systemtest.utils;

import io.restassured.http.ContentType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicUtils {

    public static final String TOPIC_COLLECTION_PATH = "/rest/topics";
    public static final String TOPIC_PATH = "/rest/topics/{topicName}";

    static final Logger log = Logger.getLogger(TopicUtils.class);
    final Config config;
    final String token;
    final Properties adminConfig;

    public TopicUtils(Config config, String token) {
        this.config = config;
        this.token = token;

        adminConfig = token != null ?
            ClientsConfig.getAdminConfigOauth(config, token) :
            ClientsConfig.getAdminConfig(config);
    }

    public void deleteAllTopics() {
        // Tests assume a clean slate - remove any existing topics
        try (Admin admin = Admin.create(adminConfig)) {
            admin.listTopics()
                .listings()
                .toCompletionStage()
                .thenApply(topics -> topics.stream().map(TopicListing::name).collect(Collectors.toList()))
                .thenComposeAsync(topicNames -> {
                    log.infof("Deleting topics: %s", topicNames);
                    return admin.deleteTopics(topicNames).all().toCompletionStage();
                })
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Process interruptted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }

    private Map<String, ?> getHeaders() {
        return token != null ?
            Map.of(HttpHeaders.AUTHORIZATION.toString(), "Bearer " + token) :
            Collections.emptyMap();
    }

    public void createTopics(List<String> names, int numPartitions, Status expectedStatus) {
        names.forEach(name -> {
            given()
                .log().ifValidationFails()
                .contentType(ContentType.JSON)
                .headers(getHeaders())
                .body(buildNewTopicRequest(name, numPartitions, Map.of("min.insync.replicas", "1")).toString())
            .when()
                .post(TOPIC_COLLECTION_PATH)
            .then()
                .log().ifValidationFails()
                .statusCode(expectedStatus.getStatusCode());
        });
    }

    public void assertNoTopicsExist() {
        given()
            .log().ifValidationFails()
            .headers(getHeaders())
        .when()
            .get(TOPIC_COLLECTION_PATH)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.OK.getStatusCode())
        .assertThat()
            .body("page", equalTo(1))
            .body("size", equalTo(10))
            .body("total", equalTo(0))
            .body("items.size()", equalTo(0));
    }

    public static JsonObject buildNewTopicRequest(String name, int numPartitions, Map<String, String> config) {
        JsonArrayBuilder configBuilder = Json.createArrayBuilder();

        config.forEach((key, value) ->
            configBuilder.add(Json.createObjectBuilder()
                              .add("key", key)
                              .add("value", value)));

        return Json.createObjectBuilder()
            .add("name", name)
            .add("settings", Json.createObjectBuilder()
                 .add("numPartitions", numPartitions)
                 .add("config", configBuilder))
            .build();
    }

    public static JsonObject buildUpdateTopicRequest(String name, int numPartitions, Map<String, String> config) {
        JsonArrayBuilder configBuilder = Json.createArrayBuilder();

        config.forEach((key, value) ->
            configBuilder.add(Json.createObjectBuilder()
                              .add("key", key)
                              .add("value", value)));

        return Json.createObjectBuilder()
            .add("name", name)
            .add("numPartitions", numPartitions)
            .add("config", configBuilder)
            .build();
    }
}
