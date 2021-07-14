package org.bf2.admin.kafka.systemtest.oauth;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.bf2.admin.kafka.systemtest.bases.TestBase;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager.UserType;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessControlListIT extends TestBase {

    static DeploymentManager deployments;

    GenericContainer<?> kafkaContainer;
    GenericContainer<?> adminContainer;

    @BeforeAll
    static void initialize(ExtensionContext extensionContext) {
        deployments = DeploymentManager.newInstance(extensionContext, true);
        deployments.getKeycloakContainer();
    }

    @BeforeEach
    void setup() {
        kafkaContainer = deployments.getKafkaContainer();
        adminContainer = deployments.getAdminContainer();
    }

    @AfterEach
    void cleanup(Vertx vertx, VertxTestContext testContext) {
        deleteAcls(vertx, testContext, UserType.OWNER, Map.of())
            .map(testContext)
            .onSuccess(VertxTestContext::completeNow)
            .onFailure(testContext::failNow);
    }

    @Test
    void testGetAclsAccessibleForOwner(Vertx vertx, VertxTestContext testContext) {
        getAcls(vertx, testContext, UserType.OWNER, Map.of(), ReturnCodes.SUCCESS.code)
            .onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testGetAclsInaccessibleForUser(Vertx vertx, VertxTestContext testContext) {
        getAcls(vertx, testContext, UserType.USER, Map.of(), ReturnCodes.FORBIDDEN.code)
            .onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void testGetAclsAccessibleForUserAfterGrant(Vertx vertx, VertxTestContext testContext) {
        Checkpoint responseBodyVerified = testContext.checkpoint();
        JsonObject newBinding = aclBinding(ResourceType.CLUSTER,
                                    "kafka-cluster",
                                    PatternType.LITERAL,
                                    "User:" + UserType.USER.getUsername(),
                                    AclOperation.DESCRIBE,
                                    AclPermissionType.ALLOW);

        createAcl(vertx, testContext, UserType.OWNER, newBinding)
            .compose(ignored -> getAcls(vertx, testContext, UserType.USER, Map.of(), ReturnCodes.SUCCESS.code))
            .compose(HttpClientResponse::body)
            .map(buffer -> new JsonObject(buffer).getJsonArray("items"))
            .map(bindings -> testContext.verify(() -> {
                assertTrue(bindings.stream().anyMatch(newBinding::equals), () ->
                    "Response " + bindings + " did not contain " + newBinding);
                responseBodyVerified.flag();
            }))
            .onComplete(testContext.succeedingThenComplete());
    }

    // Utilities

    Future<HttpClientResponse> getAcls(Vertx vertx, VertxTestContext testContext, UserType userType, Map<String, String> filters, int expectedStatusCode) {
        return adminServerRequest(vertx, testContext, HttpMethod.GET, getAclPath(filters), userType, null, expectedStatusCode);
    }

    Future<HttpClientResponse> createAcl(Vertx vertx, VertxTestContext testContext, UserType userType, JsonObject newBinding) {
        return adminServerRequest(vertx, testContext, HttpMethod.POST, getAclPath(Map.of()), userType, newBinding, ReturnCodes.TOPIC_CREATED.code);
    }

    Future<HttpClientResponse> deleteAcls(Vertx vertx, VertxTestContext testContext, UserType userType, Map<String, String> filters) {
        return adminServerRequest(vertx, testContext, HttpMethod.DELETE, getAclPath(filters), userType, null, ReturnCodes.SUCCESS.code);
    }

    String getAclPath(Map<String, String> filters) {
        StringBuilder path = new StringBuilder("/rest/acls");
        int count = 0;

        for (Map.Entry<String, String> filter : filters.entrySet()) {
            path.append(count++ > 0 ? '&' : '?');
            path.append(String.format("%s=%s", filter.getKey(), filter.getValue()));
        }

        return path.toString();
    }

    Future<HttpClientResponse> adminServerRequest(Vertx vertx,
                                                  VertxTestContext testContext,
                                                  HttpMethod method,
                                                  String path,
                                                  UserType userType,
                                                  JsonObject body,
                                                  int expectedStatusCode) {

        Checkpoint statusVerified = testContext.checkpoint();
        int port = deployments.getAdminServerPort();

        Future<HttpClientRequest> request = createHttpClient(vertx, true)
                .request(method, port, "localhost", path)
                .compose(req -> CompositeFuture.all(Future.succeededFuture(req), deployments.getAccessToken(vertx, userType)))
                .map(composite -> composite.<HttpClientRequest>resultAt(0).putHeader("Authorization", "Bearer " + composite.resultAt(1)));

        final Future<HttpClientResponse> response;

        if (body != null) {
            response = request
                    .map(req -> req.putHeader("content-type", "application/json"))
                    .compose(req -> req.send(body.toBuffer()));
        } else {
            response = request.compose(HttpClientRequest::send);
        }

        return response.compose(rsp -> {
            Promise<HttpClientResponse> promise = Promise.promise();

            testContext.verify(() -> {
                assertEquals(expectedStatusCode, rsp.statusCode());
                statusVerified.flag();
            });

            if (testContext.failed()) {
                promise.fail(testContext.causeOfFailure());
            } else {
                promise.complete(rsp);
            }

            return promise.future();
        });
    }

    JsonObject aclBinding(ResourceType resourceType,
                          String resourceName,
                          PatternType patternType,
                          String principal,
                          AclOperation operation,
                          AclPermissionType permission) {
        return new JsonObject()
                .put("resourceType", resourceType.name())
                .put("resourceName", resourceName)
                .put("patternType", patternType.name())
                .put("principal", principal)
                .put("operation", operation.name())
                .put("permission", permission.name());
    }

}
