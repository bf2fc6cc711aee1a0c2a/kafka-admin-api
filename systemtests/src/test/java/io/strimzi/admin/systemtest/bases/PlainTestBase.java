/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.bases;

import io.strimzi.admin.systemtest.deployment.AdminDeploymentManager;
import io.strimzi.admin.systemtest.json.ModelDeserializer;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;

public class PlainTestBase extends TestBase {
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = AdminDeploymentManager.getInstance();
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();

    @BeforeEach
    public void startup(Vertx vertx, VertxTestContext vertxTestContext, ExtensionContext testContext) throws Exception {
        DEPLOYMENT_MANAGER.deployPlainStack(vertxTestContext, testContext);
    }
}
