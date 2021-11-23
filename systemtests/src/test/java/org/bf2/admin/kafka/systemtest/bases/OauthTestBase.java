package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.junit5.VertxTestContext;
import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager.UserType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.ExecutionException;

@Tag(TestTag.OAUTH)
public class OauthTestBase extends TestBase {

    protected String token;

    @BeforeAll
    static void initialize(ExtensionContext extensionContext) {
        deployments = DeploymentManager.newInstance(true);
        deployments.getKeycloakContainer();
    }

    @BeforeEach
    void setup(Vertx vertx) throws InterruptedException, ExecutionException {
        this.token = deployments.getAccessTokenNow(vertx, UserType.OWNER);
        this.kafkaClient = deployments.createKafkaAdmin(token);
        deleteAllTopics();
    }

    protected void changeTokenToUnauthorized(Vertx vertx, VertxTestContext testContext) {
        this.token = deployments.getAccessTokenNow(vertx, UserType.OTHER);
    }

    protected HttpClientRequest setDefaultAuthorization(HttpClientRequest request) {
        return request.putHeader("Authorization", "Bearer " + this.token);
    }

    protected Future<HttpClientRequest> setAuthorization(HttpClientRequest request, Vertx vertx, UserType type) {
        return deployments.getAccessToken(vertx, type)
            .map(token -> request.putHeader("Authorization", "Bearer " + token));
    }
}
