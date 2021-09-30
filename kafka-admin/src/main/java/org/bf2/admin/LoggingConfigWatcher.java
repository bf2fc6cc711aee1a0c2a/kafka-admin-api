package org.bf2.admin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logmanager.LogContext;

@ApplicationScoped
public class LoggingConfigWatcher {

    private static final Logger LOGGER = LogManager.getLogger(LoggingConfigWatcher.class);

    private static final String RECONFIG_FILE_MODIFIED = "Reconfiguration triggered; reason: log configuration has been modified: {}";
    private static final String RECONFIG_FILE_ADDED = "Reconfiguration triggered; reason: previously absent log configuration is now available: {}";
    private static final String RECONFIG_FILE_MISSING = "Reconfiguration triggered; reason: previously available log configured file is no longer present: {}";

    private Map<File, Long> configuredFiles;
    private Set<File> configurationCandidates;
    private ScheduledExecutorService workerPool;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @ConfigProperty(name = "logging.config.files")
    Optional<String> files;

    @ConfigProperty(name = "logging.config.interval", defaultValue = "5")
    long period;

    public void start(@Observes StartupEvent event) {
        if (files.isEmpty() || files.get().trim().isEmpty()) {
            LOGGER.info("No log config files set to monitor");
            return;
        }

        configurationCandidates = Arrays.stream(files.get().split(","))
            .map(this::toFile)
            .collect(Collectors.toSet());

        LOGGER.info("Monitoring files: {}", configurationCandidates);

        /*
         * We mock what was done before with Log4j, now with JBoss Logging
         */
        configuredFiles = new ConcurrentHashMap<>();
        configurationCandidates.stream()
            .filter(Objects::nonNull)
            .filter(File::exists)
            .forEach(file -> configuredFiles.put(file, file.lastModified()));

        LOGGER.info("Initializing logging configuration watcher, period = {}sec", period);
        workerPool = Executors.newScheduledThreadPool(1);
        workerPool.scheduleAtFixedRate(this::monitor, period, period, TimeUnit.SECONDS);
    }

    public void stop(@Observes ShutdownEvent event) {
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }

    private File toFile(String fileUrl) {
        File configFile = null;

        try {
            configFile = new File(new URL(fileUrl).toURI());
        } catch (MalformedURLException | URISyntaxException e) {
            LOGGER.info("File name must convertable to URI: {} [{}]", fileUrl, e.getMessage());
        }

        return configFile;
    }

    private void monitor() {
        LOGGER.debug("Checking for updates to configuration files: {}", configurationCandidates);
        configurationCandidates.forEach(file -> {
            if (reconfigurationRequired(file)) {
                modify(file);
            }
        });
    }

    private void modify(File file) {
        try {
            Properties properties = new Properties();
            try (InputStream stream = Files.newInputStream(file.toPath())) {
                properties.load(stream);
            }

            LogContext context = LogContext.getLogContext();

            for (String key : properties.stringPropertyNames()) {
                if (key.equals("quarkus.log.level") || key.equals("quarkus.log.min-level")) {
                    org.jboss.logmanager.Logger root = context.getLogger("");
                    Level level = context.getLevelForName(properties.getProperty(key));
                    // TODO -- min-level handling?
                    // https://github.com/quarkusio/quarkus/blob/2.3.0.Final/core/runtime/src/main/java/io/quarkus/runtime/logging/LoggingSetupRecorder.java#L92
                    root.setLevel(level);
                } else if (key.startsWith("quarkus.log.category.")) {
                    int i1 = key.indexOf("\"");
                    int i2 = key.lastIndexOf("\"");
                    String category = key.substring(i1 + 1, i2);
                    org.jboss.logmanager.Logger logger = context.getLogger(category);
                    String type = key.substring(i2 + 2); // + 1 + dot
                    if (type.equals("level") || type.equals("mil-level")) {
                        Level level = context.getLevelForName(properties.getProperty(key));
                        // TODO -- min-level handling?
                        logger.setLevel(level);
                    }
                    // TODO -- handle other types ...
                    // https://quarkus.io/guides/logging#logging-categories
                }
            }
        } catch (IOException e) {
            LOGGER.warn("File {} cannot be read", file, e);
        }
    }

    boolean reconfigurationRequired(File configFile) {
        boolean reconfigure = false;
        boolean previouslyConfigured = configuredFiles.containsKey(configFile);

        if (Files.exists(configFile.toPath())) {
            if (previouslyConfigured) {
                long lastModified = configFile.lastModified();
                long previousLastModified = configuredFiles.get(configFile);

                if (lastModified > previousLastModified) {
                    LOGGER.info(RECONFIG_FILE_MODIFIED, configFile);
                    configuredFiles.put(configFile, lastModified);
                    reconfigure = true;
                }
            } else {
                LOGGER.info(RECONFIG_FILE_ADDED, configFile);
                configuredFiles.put(configFile, configFile.lastModified());
                reconfigure = true;
            }
        } else if (previouslyConfigured) {
            LOGGER.info(RECONFIG_FILE_MISSING, configFile);
            configuredFiles.remove(configFile);
            // reconfigure = true; // we only know how to configure changes
        }

        return reconfigure;
    }
}
