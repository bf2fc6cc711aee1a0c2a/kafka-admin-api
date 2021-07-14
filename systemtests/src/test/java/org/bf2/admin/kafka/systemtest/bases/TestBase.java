package org.bf2.admin.kafka.systemtest.bases;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bf2.admin.kafka.systemtest.IndicativeSentences;
import org.bf2.admin.kafka.systemtest.listeners.ExtensionContextParameterResolver;
import org.bf2.admin.kafka.systemtest.listeners.TestCallbackListener;
import org.bf2.admin.kafka.systemtest.listeners.TestExceptionCallbackListener;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;


@ExtendWith(TestCallbackListener.class)
@ExtendWith(ExtensionContextParameterResolver.class)
@ExtendWith(TestExceptionCallbackListener.class)
@ExtendWith(VertxExtension.class)
@DisplayNameGeneration(IndicativeSentences.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(TestBase.class);

    protected HttpClient createHttpClient(Vertx vertx, boolean isSecured) {
        HttpClientOptions options = new HttpClientOptions();
        if (isSecured) {
            options.setSsl(true);
            options.setEnabledSecureTransportProtocols(Set.of("TLSv1.3"));
            options.setPemTrustOptions(new PemTrustOptions().addCertPath(Path.of("docker", "certificates", "ca.crt").toAbsolutePath().toString()));
        }
        return vertx.createHttpClient(options);
    }

    protected void assertStrictTransportSecurity(HttpClientResponse response, VertxTestContext testContext, boolean secureTransport) {
        String hsts = response.getHeader("Strict-Transport-Security");

        if (secureTransport) {
            if (hsts == null) {
                testContext.failNow("HSTS header missing");
            } else if (!hsts.equals(String.format("max-age=%d", Duration.ofDays(365).toSeconds()))) {
                testContext.failNow("HSTS header unexpected value");
            }
        } else if (hsts != null) {
            testContext.failNow("HSTS header present on insecure response");
        }
    }

}
