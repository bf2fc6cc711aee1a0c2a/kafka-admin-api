package org.bf2.admin.kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * KafkaAdminConfigRetriever class gets configuration from envvars
 */
public class KafkaAdminConfigRetriever {

    protected final Logger log = LogManager.getLogger(KafkaAdminConfigRetriever.class);
    private static final String PREFIX = "KAFKA_ADMIN_";
    private static final String OAUTHBEARER = "OAUTHBEARER";
    private final Map<String, Object> config;

    public KafkaAdminConfigRetriever() {
        config = envVarsToAdminClientConfig(PREFIX);
        logConfiguration();
    }

    private Map<String, Object> envVarsToAdminClientConfig(String prefix) {
        Map<String, Object> envConfig = System.getenv().entrySet().stream().filter(entry -> entry.getKey().startsWith(prefix)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Object> adminClientConfig = new HashMap<>();
        if (envConfig.get(PREFIX + "BOOTSTRAP_SERVERS") == null) {
            throw new IllegalStateException("Bootstrap address has to be specified");
        }
        adminClientConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envConfig.get(PREFIX + "BOOTSTRAP_SERVERS").toString());

        // oAuth
        if (System.getenv(PREFIX + "OAUTH_ENABLED") == null ? true : Boolean.valueOf(System.getenv(PREFIX + "OAUTH_ENABLED"))) {
            log.info("oAuth enabled");
            adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            adminClientConfig.put(SaslConfigs.SASL_MECHANISM, OAUTHBEARER);
            adminClientConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        } else {
            log.info("oAuth disabled");
        }
        // admin client
        adminClientConfig.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        adminClientConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");

        return adminClientConfig;
    }

    private void logConfiguration() {
        log.info("AdminClient configuration:");
        config.entrySet().forEach(entry -> {
            log.info("\t{} = {}", entry.getKey(), entry.getValue());
        });
    }

    public static boolean isOauthEnabled(Map<String, Object> config) {
        return OAUTHBEARER.equals(config.get(SaslConfigs.SASL_MECHANISM));
    }

    public Map<String, Object> getAcConfig() {
        return new HashMap<>(config);
    }
}

