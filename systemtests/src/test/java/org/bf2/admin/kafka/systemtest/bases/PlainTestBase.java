package org.bf2.admin.kafka.systemtest.bases;

import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.deployment.AdminDeploymentManager;
import org.bf2.admin.kafka.systemtest.json.ModelDeserializer;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

@Tag(TestTag.PLAIN)
public class PlainTestBase extends TestBase {
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = AdminDeploymentManager.getInstance();
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();

    @BeforeEach
    public void startup(Vertx vertx, VertxTestContext vertxTestContext, ExtensionContext testContext) throws Exception {
        DEPLOYMENT_MANAGER.deployPlainStack(vertxTestContext, testContext);
    }

    protected HttpClient createHttpClient(Vertx vertx) {
        return super.createHttpClient(vertx, false);
    }

    protected void assertStrictTransportSecurityDisabled(HttpClientResponse response, VertxTestContext testContext) {
        assertStrictTransportSecurity(response, testContext, false);
    }
}
