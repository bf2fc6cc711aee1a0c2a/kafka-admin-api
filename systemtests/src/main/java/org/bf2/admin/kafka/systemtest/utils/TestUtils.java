package org.bf2.admin.kafka.systemtest.utils;

import org.bf2.admin.kafka.systemtest.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {
    protected static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    public static void logWithSeparator(String pattern, String text) {
        LOGGER.info("=======================================================================");
        LOGGER.info(pattern, text);
        LOGGER.info("=======================================================================");
    }

    public static void logDeploymentPhase(String text) {
        LOGGER.info("-----------------------------------------");
        LOGGER.info(text);
        LOGGER.info("-----------------------------------------");
    }

    public static Path getLogPath(String folderName, ExtensionContext context) {
        String testMethod = context.getDisplayName();
        Class<?> testClass = context.getTestClass().get();
        return getLogPath(folderName, testClass, testMethod);
    }

    public static Path getLogPath(String folderName, Class<?> testClass, String testMethod) {
        Path path = Environment.LOG_DIR.resolve(Paths.get(folderName, testClass.getName()));
        if (testMethod != null) {
            path = path.resolve(testMethod.replace("(", "").replace(")", ""));
        }
        return path;
    }
}
