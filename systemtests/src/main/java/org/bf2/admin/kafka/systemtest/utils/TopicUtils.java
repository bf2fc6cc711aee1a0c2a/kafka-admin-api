package org.bf2.admin.kafka.systemtest.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;
import org.jboss.logging.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

public class TopicUtils {

    static final Logger log = Logger.getLogger(TopicUtils.class);
    final String bootstrapServers;
    final Properties adminConfig;

    public TopicUtils(String bootstrapServers, String token) {
        this.bootstrapServers = bootstrapServers;

        adminConfig = token != null ?
            ClientsConfig.getAdminConfigOauth(token, bootstrapServers) :
            ClientsConfig.getAdminConfig(bootstrapServers);
    }

    public void deleteAllTopics() {
        // Tests assume a clean slate - remove any existing topics
        try (Admin admin = Admin.create(adminConfig)) {
            admin.listTopics()
                .listings()
                .toCompletionStage()
                .thenApply(topics -> topics.stream().map(TopicListing::name).collect(Collectors.toList()))
                .thenComposeAsync(topicNames -> {
                    log.infof("Deleting topics: %s", topicNames);
                    return admin.deleteTopics(topicNames).all().toCompletionStage();
                })
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Process interruptted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }
}
