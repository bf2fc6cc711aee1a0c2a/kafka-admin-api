/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.bases;

import io.strimzi.admin.systemtest.deployment.AdminDeploymentManager;
import io.strimzi.admin.systemtest.listeners.ExtensionContextParameterResolver;
import io.strimzi.admin.systemtest.IndicativeSentences;
import io.strimzi.admin.systemtest.listeners.TestCallbackListener;
import io.strimzi.admin.systemtest.listeners.TestExceptionCallbackListener;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(TestCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(VertxExtension.class)
@DisplayNameGeneration(IndicativeSentences.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(TestBase.class);
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = AdminDeploymentManager.getInstance();
}
