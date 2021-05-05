package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.IndicativeSentences;
import org.bf2.admin.kafka.systemtest.deployment.AdminDeploymentManager;
import org.bf2.admin.kafka.systemtest.listeners.ExtensionContextParameterResolver;
import org.bf2.admin.kafka.systemtest.listeners.TestCallbackListener;
import org.bf2.admin.kafka.systemtest.listeners.TestExceptionCallbackListener;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.Set;


@ExtendWith(TestCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(VertxExtension.class)
@DisplayNameGeneration(IndicativeSentences.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(TestBase.class);
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = AdminDeploymentManager.getInstance();

    protected HttpClient createHttpsClient(Vertx vertx) {
        HttpClientOptions options = new HttpClientOptions();
        options.setSsl(true);
        options.setEnabledSecureTransportProtocols(Set.of("TLSv1.3"));
        options.setPemTrustOptions(new PemTrustOptions().addCertPath(Path.of("docker", "certificates", "ca.crt").toAbsolutePath().toString()));
        return vertx.createHttpClient(options);
    }
}
