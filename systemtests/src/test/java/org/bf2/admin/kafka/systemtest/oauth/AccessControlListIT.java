package org.bf2.admin.kafka.systemtest.oauth;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.bf2.admin.kafka.admin.AccessControlOperations;
import org.bf2.admin.kafka.admin.model.Types;
import org.bf2.admin.kafka.systemtest.bases.OauthTestBase;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager.UserType;
import org.bf2.admin.kafka.systemtest.enums.ReturnCodes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.bf2.admin.kafka.systemtest.Environment.CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessControlListIT extends OauthTestBase {

    static final String PARAMETERIZED_TEST_NAME =
            ParameterizedTest.DISPLAY_NAME_PLACEHOLDER + "-" + ParameterizedTest.DEFAULT_DISPLAY_NAME;

    static final String SORT_ASC = "asc";
    static final String SORT_DESC = "desc";

    @AfterEach
    void cleanup(Vertx vertx, VertxTestContext testContext) {
        Checkpoint statusVerified = testContext.checkpoint();
        deleteAcls(vertx, testContext, statusVerified, UserType.OWNER, Map.of())
            .map(testContext)
            .onSuccess(VertxTestContext::completeNow)
            .onFailure(testContext::failNow);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @CsvSource({
        "OWNER,   SUCCESS",
        "USER,    FORBIDDEN",
        "OTHER,   FORBIDDEN",
        "INVALID, UNAUTHORIZED"
    })
    void testGetAclsByUserType(UserType userType, ReturnCodes expectedStatus, Vertx vertx, VertxTestContext testContext) {
        Checkpoint statusVerified = testContext.checkpoint();

        getAcls(vertx, testContext, statusVerified, userType, Map.of(), expectedStatus)
            .onFailure(testContext::failNow);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @CsvSource({
        "OWNER,   SUCCESS, SUCCESS",
        "USER,    FORBIDDEN, SUCCESS",
        "OTHER,   FORBIDDEN, FORBIDDEN", // OTHER user always restricted from cluster (per Keycloak RBAC)
        "INVALID, UNAUTHORIZED, UNAUTHORIZED"
    })
    void testGetAclsByUserTypeWithGrant(UserType userType, ReturnCodes beforeStatus, ReturnCodes afterStatus,
                                 Vertx vertx, VertxTestContext testContext) {

        Checkpoint statusVerified = testContext.checkpoint(3);
        Checkpoint responseBodyVerified = testContext.checkpoint();
        JsonObject newBinding = aclBinding(ResourceType.CLUSTER,
                                    "kafka-cluster",
                                    PatternType.LITERAL,
                                    "User:" + UserType.USER.getUsername(),
                                    AclOperation.DESCRIBE,
                                    AclPermissionType.ALLOW);

        Future<HttpClientResponse> response;

        response = getAcls(vertx, testContext, statusVerified, userType, Map.of(), beforeStatus)
            .compose(ignored -> createAcl(vertx, testContext, statusVerified, UserType.OWNER, newBinding))
            .compose(ignored -> getAcls(vertx, testContext, statusVerified, userType, Map.of(), afterStatus));

        if (afterStatus == ReturnCodes.SUCCESS) {
            response.compose(HttpClientResponse::body)
                .map(buffer -> new JsonObject(buffer).getJsonArray("items"))
                .map(bindings -> testContext.verify(() -> {
                    assertTrue(bindings.stream().anyMatch(newBinding::equals), () ->
                        "Response " + bindings + " did not contain " + newBinding);
                    responseBodyVerified.flag();
                }))
                .onFailure(testContext::failNow);
        } else {
            responseBodyVerified.flag();
            response.onFailure(testContext::failNow);
        }
    }

    @Test
    void testCreateAclsDeniedInvalid(Vertx vertx, VertxTestContext testContext) {
        JsonObject newBinding = aclBinding(ResourceType.CLUSTER,
                                           "kafka-cluster",
                                           PatternType.LITERAL,
                                           "User:" + UserType.USER.getUsername(),
                                           AclOperation.ALL,
                                           AclPermissionType.ALLOW);

        Checkpoint statusVerified = testContext.checkpoint();
        Checkpoint responseBodyVerified = testContext.checkpoint();

        createAcl(vertx, testContext, statusVerified, UserType.OWNER, newBinding, ReturnCodes.FAILED_REQUEST)
            .compose(HttpClientResponse::body)
            .map(buffer -> new JsonObject(buffer))
            .map(response -> testContext.verify(() -> {
                assertEquals(400, response.getInteger("code"));
                assertEquals(AccessControlOperations.INVALID_ACL_RESOURCE_OPERATION, response.getString("error_message"));
                responseBodyVerified.flag();
            }))
            .onFailure(testContext::failNow);
    }

    @Test
    void testGetAclsByUserIncludesWildcard(Vertx vertx, VertxTestContext testContext) {
        String principal = "User:" + UserType.USER.getUsername();
        JsonObject binding1 = aclBinding(ResourceType.TOPIC, "user_topic", PatternType.LITERAL, principal, AclOperation.READ, AclPermissionType.ALLOW);
        JsonObject binding2 = aclBinding(ResourceType.TOPIC, "public_topic", PatternType.LITERAL, "User:*", AclOperation.READ, AclPermissionType.ALLOW);
        List<JsonObject> newBindings = List.of(binding1, binding2);

        Checkpoint statusVerified = testContext.checkpoint(3);
        Checkpoint responseBodyVerified = testContext.checkpoint();

        createAcls(vertx, testContext, statusVerified, UserType.OWNER, newBindings)
            .compose(ignored -> getAcls(vertx, testContext, statusVerified, UserType.OWNER, Map.of("principal", principal)))
            .compose(HttpClientResponse::body)
            .map(buffer -> new JsonObject(buffer).getJsonArray("items"))
            .map(bindings -> testContext.verify(() -> {
                assertEquals(2, bindings.size());
                // Objects internal to the array are stored as Maps - iteration forces conversion to JsonObject
                List<JsonObject> createdBindings = bindings.stream().map(JsonObject.class::cast).collect(Collectors.toList());
                assertTrue(newBindings.stream().allMatch(createdBindings::contains), () ->
                    "Response " + bindings + " did not contain one of " + newBindings);
                responseBodyVerified.flag();
            }))
            .onFailure(testContext::failNow);
    }

    @Test
    void testGetAclsByWildcardExcludesUser(Vertx vertx, VertxTestContext testContext) {
        String principal = "User:" + UserType.USER.getUsername();
        JsonObject binding1 = aclBinding(ResourceType.TOPIC, "user_topic", PatternType.LITERAL, principal, AclOperation.READ, AclPermissionType.ALLOW);
        JsonObject binding2 = aclBinding(ResourceType.TOPIC, "public_topic", PatternType.LITERAL, "User:*", AclOperation.READ, AclPermissionType.ALLOW);
        List<JsonObject> newBindings = List.of(binding1, binding2);

        Checkpoint statusVerified = testContext.checkpoint(3);
        Checkpoint responseBodyVerified = testContext.checkpoint();

        createAcls(vertx, testContext, statusVerified, UserType.OWNER, newBindings)
            .compose(ignored -> getAcls(vertx, testContext, statusVerified, UserType.OWNER, Map.of("principal", "User:*")))
            .compose(HttpClientResponse::body)
            .map(buffer -> new JsonObject(buffer).getJsonArray("items"))
            .map(bindings -> testContext.verify(() -> {
                assertEquals(1, bindings.size());
                assertTrue(bindings.stream().anyMatch(binding2::equals), () ->
                    "Response " + bindings + " did not contain " + binding2);
                responseBodyVerified.flag();
            }))
            .onFailure(testContext::failNow);
    }

    @Test
    void testGetAclsSortsDenyFirst(Vertx vertx, VertxTestContext testContext) {
        String principal = "User:" + UserType.USER.getUsername();

        List<JsonObject> newBindings = IntStream.range(0, 20)
                .mapToObj(index -> aclBinding(ResourceType.TOPIC, "topic" + index,
                                              PatternType.LITERAL,
                                              principal,
                                              AclOperation.READ,
                                              index % 2 == 0 ? AclPermissionType.ALLOW : AclPermissionType.DENY))
                .collect(Collectors.toList());

        Checkpoint statusVerified = testContext.checkpoint(newBindings.size() + 1);
        Checkpoint responseBodyVerified = testContext.checkpoint();

        createAcls(vertx, testContext, statusVerified, UserType.OWNER, newBindings)
            .compose(ignored -> getAcls(vertx, testContext, statusVerified, UserType.OWNER, Map.of("page", "1", "size", "10")))
            .compose(HttpClientResponse::body)
            .map(buffer -> new JsonObject(buffer))
            .map(response -> testContext.verify(() -> {
                assertEquals(20, response.getInteger("total"));
                assertEquals(1, response.getInteger("page"));
                assertEquals(10, response.getInteger("size"));

                JsonArray bindings = response.getJsonArray("items");
                assertEquals(10, bindings.size());
                assertTrue(bindings.stream()
                           .map(JsonObject.class::cast)
                           .map(b -> b.getString("permission"))
                           .map(AclPermissionType::valueOf)
                           .allMatch(AclPermissionType.DENY::equals), () ->
                    "Response " + bindings + " were not all DENY");
                responseBodyVerified.flag();
            }))
            .onFailure(testContext::failNow);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @CsvSource({
        Types.AclBinding.PROP_PERMISSION + "," + SORT_ASC,
        Types.AclBinding.PROP_PERMISSION + "," + SORT_DESC,
        Types.AclBinding.PROP_RESOURCE_TYPE + "," + SORT_ASC,
        Types.AclBinding.PROP_RESOURCE_TYPE + "," + SORT_DESC,
        Types.AclBinding.PROP_PATTERN_TYPE + "," + SORT_ASC,
        Types.AclBinding.PROP_PATTERN_TYPE + "," + SORT_DESC,
        Types.AclBinding.PROP_OPERATION + "," + SORT_ASC,
        Types.AclBinding.PROP_OPERATION + "," + SORT_DESC,
        Types.AclBinding.PROP_PRINCIPAL + "," + SORT_ASC,
        Types.AclBinding.PROP_PRINCIPAL + "," + SORT_DESC,
        Types.AclBinding.PROP_RESOURCE_NAME + "," + SORT_ASC,
        Types.AclBinding.PROP_RESOURCE_NAME + "," + SORT_DESC,
    })
    void testGetAclsOrderByProperies(String orderKey, String order, Vertx vertx, VertxTestContext testContext) {
        JsonObject allowedResourceOperations = new JsonObject(CONFIG.getProperty("systemtests.kafka.admin.acl.resource-operations"));

        List<JsonObject> newBindings = Stream.of(new JsonObject())
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_PERMISSION, AclPermissionType.ALLOW, AclPermissionType.DENY))
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_RESOURCE_TYPE, ResourceType.TOPIC, ResourceType.GROUP, ResourceType.CLUSTER, ResourceType.TRANSACTIONAL_ID))
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_PATTERN_TYPE, PatternType.LITERAL, PatternType.PREFIXED))
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_OPERATION, AclOperation.READ,
                                     AclOperation.ALL, AclOperation.ALTER, AclOperation.DELETE,
                                     AclOperation.CREATE, AclOperation.ALTER_CONFIGS,
                                     AclOperation.DESCRIBE, AclOperation.DESCRIBE_CONFIGS, AclOperation.WRITE))
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_PRINCIPAL, "User:{uuid}"))
            .flatMap(binding -> join(binding, Types.AclBinding.PROP_RESOURCE_NAME, "resource-{uuid}"))
            .filter(binding -> {
                JsonArray operations = allowedResourceOperations.getJsonArray(binding.getString(Types.AclBinding.PROP_RESOURCE_TYPE).toLowerCase(Locale.US));
                return operations.contains(binding.getString(Types.AclBinding.PROP_OPERATION).toLowerCase(Locale.US));
            })
            .map(binding -> {
                if (ResourceType.CLUSTER.name().equals(binding.getString(Types.AclBinding.PROP_RESOURCE_TYPE))) {
                    // Only value allowed is "kafka-cluster"
                    binding.put(Types.AclBinding.PROP_RESOURCE_NAME, "kafka-cluster");
                }
                return binding;
            })
            .distinct()
            .collect(Collectors.toList());

        List<String> expectedValues = newBindings.stream()
                .map(JsonObject.class::cast)
                .map(b -> b.getString(orderKey))
                .collect(Collectors.toList());

        Collections.sort(expectedValues);

        if (SORT_DESC.equals(order)) {
            Collections.reverse(expectedValues);
        }

        Checkpoint statusVerified = testContext.checkpoint();
        Checkpoint responseBodyVerified = testContext.checkpoint();

        final int expectedTotal = newBindings.size();
        final int pageSize = expectedTotal + 1;
        final var queryParams = Map.of("page", "1", "size", String.valueOf(pageSize), "orderKey", orderKey, "order", order);

        /*
         * Due to the number of ACLs created for this case (> 200), using the
         * bulk API directly is necessary.
         */
        kafkaClient.createAcls(newBindings.stream()
                               .map(Types.AclBinding::fromJsonObject)
                               .map(Types.AclBinding::toKafkaBinding)
                               .collect(Collectors.toList()))
            .all()
            .whenComplete((result, error) -> {
                if (error != null) {
                    testContext.failNow(error);
                } else {
                    getAcls(vertx, testContext, statusVerified, UserType.OWNER, queryParams)
                        .compose(HttpClientResponse::body)
                        .map(buffer -> new JsonObject(buffer))
                        .map(response -> testContext.verify(() -> {
                            assertEquals(expectedTotal, response.getInteger("total"));
                            assertEquals(pageSize, response.getInteger("size"));
                            assertEquals(1, response.getInteger("page"));

                            JsonArray bindings = response.getJsonArray("items");
                            assertEquals(expectedTotal, bindings.size());

                            List<String> responseValues = bindings.stream()
                                    .map(JsonObject.class::cast)
                                    .map(b -> b.getString(orderKey))
                                    .collect(Collectors.toList());

                            assertEquals(expectedValues, responseValues, "Unexpected response order");
                            responseBodyVerified.flag();
                        }))
                        .onFailure(testContext::failNow);
                }
            });
    }

    // Utilities

    Future<HttpClientResponse> getAcls(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, Map<String, String> filters, ReturnCodes expectedStatus) {
        return adminServerRequest(vertx, testContext, statusVerified, HttpMethod.GET, getAclPath(filters), userType, null, expectedStatus);
    }

    Future<HttpClientResponse> getAcls(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, Map<String, String> filters) {
        return getAcls(vertx, testContext, statusVerified, userType, filters, ReturnCodes.SUCCESS);
    }

    Future<HttpClientResponse> createAcl(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, JsonObject newBinding, ReturnCodes expectedStatus) {
        return adminServerRequest(vertx, testContext, statusVerified, HttpMethod.POST, getAclPath(Map.of()), userType, newBinding, expectedStatus);
    }

    Future<HttpClientResponse> createAcl(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, JsonObject newBinding) {
        return createAcl(vertx, testContext, statusVerified, userType, newBinding, ReturnCodes.TOPIC_CREATED);
    }

    Future<Void> createAcls(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, List<JsonObject> newBindings) {
        return CompositeFuture.all(newBindings.stream()
                                   .map(binding -> createAcl(vertx, testContext, statusVerified, userType, binding, ReturnCodes.TOPIC_CREATED))
                                   .collect(Collectors.toList()))
                .compose(composite -> Future.<Void>succeededFuture());
    }

    Future<HttpClientResponse> deleteAcls(Vertx vertx, VertxTestContext testContext, Checkpoint statusVerified, UserType userType, Map<String, String> filters) {
        return adminServerRequest(vertx, testContext, statusVerified, HttpMethod.DELETE, getAclPath(filters), userType, null, ReturnCodes.SUCCESS);
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
                                                  Checkpoint statusVerified,
                                                  HttpMethod method,
                                                  String path,
                                                  UserType userType,
                                                  JsonObject body,
                                                  ReturnCodes expectedStatus) {

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
                assertEquals(expectedStatus.code, rsp.statusCode(), () -> {
                    try {
                        return "Unexpected status; body=" + rsp.body().toCompletionStage().toCompletableFuture().get().toString();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });
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
                .put(Types.AclBinding.PROP_RESOURCE_TYPE, resourceType.name())
                .put(Types.AclBinding.PROP_RESOURCE_NAME, resourceName)
                .put(Types.AclBinding.PROP_PATTERN_TYPE, patternType.name())
                .put(Types.AclBinding.PROP_PRINCIPAL, principal)
                .put(Types.AclBinding.PROP_OPERATION, operation.name())
                .put(Types.AclBinding.PROP_PERMISSION, permission.name());
    }

    Stream<JsonObject> join(JsonObject base, String newKey, Object... values) {
        List<JsonObject> results = new ArrayList<>(values.length);

        for (Object value : values) {
            JsonObject copy = base.copy();
            String newValue = value.toString();

            if (newValue != null && newValue.contains("{uuid}")) {
                newValue = newValue.replace("{uuid}", UUID.randomUUID().toString());
            }

            copy.put(newKey, newValue);
            results.add(copy);
        }

        return results.stream();
    }
}
