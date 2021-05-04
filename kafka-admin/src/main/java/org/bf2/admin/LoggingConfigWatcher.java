package org.bf2.admin;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;

public class LoggingConfigWatcher {

    private static final Logger LOGGER = LogManager.getLogger(LoggingConfigWatcher.class);

    private static final String RECONFIG_FILE_MODIFIED = "Reconfiguration triggered; reason: log configuration has been modified: {}";
    private static final String RECONFIG_FILE_ADDED = "Reconfiguration triggered; reason: previously absent log configuration is now available: {}";
    private static final String RECONFIG_FILE_MISSING = "Reconfiguration triggered; reason: previously available log configured file is no longer present: {}";

    private final Duration monitorInterval;
    private final ScheduledExecutorService workerPool = Executors.newScheduledThreadPool(1);
    private final Set<File> configurationCandidates;
    private final Map<File, Long> configuredFiles = new ConcurrentHashMap<>();

    public LoggingConfigWatcher() {
        this.monitorInterval = Duration.ofSeconds(Long.getLong("logging.config.interval", 5));

        String files = System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "");

        this.configurationCandidates = files.trim().isEmpty() ?
                Collections.emptySet() : Arrays.stream(files.split(","))
                    .map(this::toFile)
                    .collect(Collectors.toSet());
    }

    public void initialize() {
        /*
         * Log4J will use all available files when the context is started.
         * Load existing files and their modification time to the configuredFiles
         * map to avoid reconfiguring Log4J the first time `monitor` executes.
         */
        this.configurationCandidates.stream()
            .filter(file -> Files.exists(file.toPath()))
            .forEach(file -> configuredFiles.put(file, file.lastModified()));

        long period = monitorInterval.toMillis();
        LOGGER.info("Initializing logging configuration watcher, period = {}ms", period);
        workerPool.scheduleAtFixedRate(this::monitor, period, period, TimeUnit.MILLISECONDS);
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
        configurationCandidates.stream()
            .filter(this::reconfigurationRequired)
            .findFirst()
            .ifPresent(file -> Configurator.reconfigure());
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
            reconfigure = true;
        }

        return reconfigure;
    }
}
