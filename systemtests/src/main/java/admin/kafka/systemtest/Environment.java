package admin.kafka.systemtest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class Environment {
    public static final HashMap<String, String> CONTAINER_LABEL = new HashMap<String, String>() {{
            put("app", "kafka-admin-api");
        }};
    private static final String LOG_DIR_ENV = "LOG_DIR";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm");

    public static final String SUITE_ROOT = System.getProperty("user.dir");
    public static final Path LOG_DIR = (System.getenv(LOG_DIR_ENV) == null ?
            Paths.get(SUITE_ROOT, "target", "logs") : Paths.get(System.getenv(LOG_DIR_ENV)))
            .resolve("test-run-" + DATE_FORMAT.format(LocalDateTime.now()));


}
