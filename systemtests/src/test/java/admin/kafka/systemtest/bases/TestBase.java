package admin.kafka.systemtest.bases;

import admin.kafka.systemtest.IndicativeSentences;
import admin.kafka.systemtest.deployment.AdminDeploymentManager;
import admin.kafka.systemtest.listeners.ExtensionContextParameterResolver;
import admin.kafka.systemtest.listeners.TestCallbackListener;
import admin.kafka.systemtest.listeners.TestExceptionCallbackListener;
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
