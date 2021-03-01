/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.logs;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import io.strimzi.admin.systemtest.Environment;
import io.strimzi.admin.systemtest.utils.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LogCollector {
    protected static final Logger LOGGER = LogManager.getLogger(LogCollector.class);
    private static LogCollector logCollector = null;
    private static DockerClient dockerClient;
    public static synchronized LogCollector getInstance() {
        if (logCollector == null) {
            logCollector = new LogCollector();
        }
        return logCollector;
    }

    private LogCollector() {
        dockerClient = DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).build();
    }

    public void collectLogs(ExtensionContext testContext) throws InterruptedException, IOException {
        Path logPath = TestUtils.getLogPath(Environment.LOG_DIR.resolve("failedTest").toString(), testContext);
        Files.createDirectories(logPath);
        LOGGER.info("Saving container logs to {}", logPath.toString());
        List<Container> containers = dockerClient.listContainersCmd().withLabelFilter(Collections.singletonMap("test-ident", testContext.getUniqueId())).exec();

        for (Container container : containers) {
            DockerLogCallback dockerLogCallback = new DockerLogCallback();
            dockerClient.logContainerCmd(container.getId()).withTailAll().withStdOut(true).withStdErr(true).exec(dockerLogCallback);
            dockerLogCallback.awaitCompletion(1, TimeUnit.SECONDS);
            Files.write(logPath.resolve(container.getId() + ".log"), dockerLogCallback.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
