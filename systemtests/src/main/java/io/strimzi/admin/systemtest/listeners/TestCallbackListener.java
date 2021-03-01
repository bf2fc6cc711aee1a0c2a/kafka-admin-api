/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.listeners;

import io.strimzi.admin.systemtest.deployment.AdminDeploymentManager;
import io.strimzi.admin.systemtest.utils.TestUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestCallbackListener implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback {
    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        TestUtils.logWithSeparator("-> Running test class: {}", extensionContext.getRequiredTestClass().getName());
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        TestUtils.logWithSeparator("-> Running test method: {}", extensionContext.getDisplayName());
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        TestUtils.logWithSeparator("-> End of test class: {}", extensionContext.getRequiredTestClass().getName());
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        TestUtils.logWithSeparator("-> End of test method: {}", extensionContext.getDisplayName());
        AdminDeploymentManager.getInstance().teardown(extensionContext);
    }
}
