package org.bf2.admin.kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * KafkaAdminConfigRetriever class gets configuration from envvars
 */
public class KafkaAdminConfigRetriever {

    protected final Logger log = LogManager.getLogger(KafkaAdminConfigRetriever.class);

    private static final String PREFIX = "KAFKA_ADMIN_";
    private static final String OAUTHBEARER = "OAUTHBEARER";
    private static final String DEFAULT_TLS_VERSION = "TLSv1.3";

    private static final String OAUTH_ENABLED_ENV = PREFIX + "OAUTH_ENABLED";
    private static final String OAUTH_JWKS_ENDPOINT_URI = PREFIX + "OAUTH_JWKS_ENDPOINT_URI";
    private static final String OAUTH_VALID_ISSUER_URI = PREFIX + "OAUTH_VALID_ISSUER_URI";
    private static final String OAUTH_TOKEN_ENDPOINT_URI = PREFIX + "OAUTH_TOKEN_ENDPOINT_URI";

    private static final String TLS_CERT = PREFIX + "TLS_CERT";
    private static final String TLS_KEY = PREFIX + "TLS_KEY";
    private static final String TLS_VERSION = PREFIX + "TLS_VERSION";

    private static final boolean OAUTH_ENABLED = System.getenv(OAUTH_ENABLED_ENV) == null || Boolean.valueOf(System.getenv(OAUTH_ENABLED_ENV));

    private final Map<String, Object> adminClientConfig;

    public KafkaAdminConfigRetriever() {
        adminClientConfig = envVarsToAdminClientConfig(PREFIX);
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
        if (OAUTH_ENABLED) {
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
        adminClientConfig.entrySet().forEach(entry -> {
            log.info("\t{} = {}", entry.getKey(), entry.getValue());
        });
    }

    public static boolean isOauthEnabled() {
        return OAUTH_ENABLED;
    }

    public Map<String, Object> getAcConfig() {
        return new HashMap<>(adminClientConfig);
    }

    public String getOauthJwksEndpointUri() {
        return System.getenv(OAUTH_JWKS_ENDPOINT_URI);
    }

    public String getOauthValidIssuerUri() {
        return System.getenv(OAUTH_VALID_ISSUER_URI);
    }

    public String getOauthTokenEndpointUri() {
        return System.getenv(OAUTH_TOKEN_ENDPOINT_URI);
    }

    public String getTlsCertificate() {
        return System.getenv(TLS_CERT);
    }

    public String getTlsKey() {
        return System.getenv(TLS_KEY);
    }

    public Set<String> getTlsVersions() {
        return Set.of(System.getenv().getOrDefault(TLS_VERSION, DEFAULT_TLS_VERSION).split(","));
    }

    public String getCorsAllowPattern() {
        return System.getenv().getOrDefault("CORS_ALLOW_LIST_REGEX", "(https?:\\/\\/localhost(:\\d*)?)");
    }
}

