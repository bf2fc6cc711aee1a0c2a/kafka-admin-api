package org.bf2.admin.kafka.systemtest.plain;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import org.bf2.admin.kafka.systemtest.TestPlainProfile;
import org.bf2.admin.kafka.systemtest.utils.RecordUtils;
import org.bf2.admin.kafka.systemtest.utils.TopicUtils;
import org.eclipse.microprofile.config.Config;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.core.Response.Status;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.stringContainsInOrder;

@QuarkusTest
@TestProfile(TestPlainProfile.class)
class RecordEndpointTestIT {

    @Inject
    Config config;

    TopicUtils topicUtils;
    RecordUtils recordUtils;

    @BeforeEach
    void setup() {
        topicUtils = new TopicUtils(config, null);
        topicUtils.deleteAllTopics();
        recordUtils = new RecordUtils(config, null);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void testProduceRecordWithPartitionAndTimestamp(int partition) {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 2, Status.CREATED);
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneOffset.UTC).minusDays(10).withNano(0);

        given()
            .log().ifValidationFails()
            .contentType(ContentType.JSON)
            .body(recordUtils.buildRecordRequest(partition, timestamp, null, null, "record value").toString())
        .when()
            .post(RecordUtils.RECORDS_PATH, topicName)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.CREATED.getStatusCode())
            .header("Location", stringContainsInOrder("/api/v1/topics/" + topicName + "/records", "partition=" + partition))
            .body("partition", equalTo(partition))
            .body("timestamp", equalTo(timestamp.toString()))
            .body("value", equalTo("record value"))
            .body("offset", greaterThanOrEqualTo(0));
    }

    @Test
    void testConsumeRecordFromInvalidTopic() {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 2, Status.CREATED);

        given()
            .log().ifValidationFails()
        .when()
            .get(RecordUtils.RECORDS_PATH, UUID.randomUUID().toString())
        .then()
            .log().ifValidationFails()
            .statusCode(Status.NOT_FOUND.getStatusCode())
            .body("code", equalTo(404))
            .body("error_message", equalTo("No such topic"));
    }

    @Test
    void testConsumeRecordFromInvalidPartition() {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 2, Status.CREATED);

        given()
            .log().ifValidationFails()
            .queryParam("partition", -1)
        .when()
            .get(RecordUtils.RECORDS_PATH, topicName)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.BAD_REQUEST.getStatusCode())
            .body("code", equalTo(400))
            .body("error_message", equalTo(String.format("No such partition for topic %s: %d", topicName, -1)));
    }

    @ParameterizedTest
    @CsvSource({
        "2000-01-01T00:00:00.000Z, 2000-01-02T00:00:00.000Z, 2000-01-01T00:00:00.000Z, 2",
        "2000-01-01T00:00:00.000Z, 2000-01-02T00:00:00.000Z, 2000-01-02T00:00:00.000Z, 1",
        "2000-01-01T00:00:00.000Z, 2000-01-02T00:00:00.000Z, 2000-01-03T00:00:00.000Z, 0"
    })
    void testConsumeRecordsAsOfTimestamp(ZonedDateTime ts1, ZonedDateTime ts2, ZonedDateTime tsSearch, int expectedResults) {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 2, Status.CREATED);
        recordUtils.produceRecord(topicName, ts1, null, "the-key1", "the-value1");
        recordUtils.produceRecord(topicName, ts2, null, "the-key2", "the-value2");

        given()
            .log().ifValidationFails()
            .queryParam("timestamp", tsSearch.toString())
        .when()
            .get(RecordUtils.RECORDS_PATH, topicName)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.OK.getStatusCode())
            .body("total", equalTo(expectedResults))
            .body("items", hasSize(expectedResults));
    }

    @ParameterizedTest
    @CsvSource({
        "0, 3",
        "1, 2",
        "2, 1",
        "3, 0",
        "4, 0"
    })
    void testConsumeRecordsByStartingOffset(int startingOffset, int expectedResults) {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 1, Status.CREATED); // single partition
        for (int i = 0; i < 3; i++) {
            recordUtils.produceRecord(topicName, null, null, "the-key-" + i, "the-value-" + i);
        }

        given()
            .log().ifValidationFails()
            .queryParam("partition", 0)
            .queryParam("offset", startingOffset)
        .when()
            .get(RecordUtils.RECORDS_PATH, topicName)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.OK.getStatusCode())
            .body("total", equalTo(expectedResults))
            .body("items", hasSize(expectedResults));
    }

    @Test
    void testConsumeRecordsIncludeOnlyHeaders() {
        final String topicName = UUID.randomUUID().toString();
        topicUtils.createTopics(List.of(topicName), 2, Status.CREATED);
        for (int i = 0; i < 3; i++) {
            recordUtils.produceRecord(topicName, null, Map.of("h1", "h1-value-" + i), "the-key-" + i, "the-value-" + i);
        }

        given()
            .log().ifValidationFails()
            .param("include", "headers")
        .when()
            .get(RecordUtils.RECORDS_PATH, topicName)
        .then()
            .log().ifValidationFails()
            .statusCode(Status.OK.getStatusCode())
            .body("total", equalTo(3))
            .body("items", hasSize(3))
            .body("items", everyItem(Matchers.aMapWithSize(1)))
            .body("items.headers", everyItem(hasKey("h1")));
    }


}