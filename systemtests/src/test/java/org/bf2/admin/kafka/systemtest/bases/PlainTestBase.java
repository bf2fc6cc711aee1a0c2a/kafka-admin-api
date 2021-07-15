package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.bf2.admin.kafka.systemtest.TestTag;
import org.bf2.admin.kafka.systemtest.deployment.DeploymentManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.ExecutionException;

@Tag(TestTag.PLAIN)
public class PlainTestBase extends TestBase {

    @BeforeAll
    static void initialize(ExtensionContext extensionContext) {
        deployments = DeploymentManager.newInstance(extensionContext, false);
    }

    @BeforeEach
    void setup(Vertx vertx) throws InterruptedException, ExecutionException {
        this.kafkaClient = deployments.createKafkaAdmin();
        deleteAllTopics();
    }

    protected HttpClient createHttpClient(Vertx vertx) {
        return super.createHttpClient(vertx, false);
    }

}
