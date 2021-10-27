package org.bf2.admin.kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Base64;
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

    public static final String BOOTSTRAP_SERVERS = PREFIX + "BOOTSTRAP_SERVERS";
    public static final String BASIC_ENABLED = PREFIX + "BASIC_ENABLED";
    public static final String OAUTH_ENABLED = PREFIX + "OAUTH_ENABLED";
    public static final String OAUTH_TRUSTED_CERT = PREFIX + "OAUTH_TRUSTED_CERT";
    public static final String OAUTH_JWKS_ENDPOINT_URI = PREFIX + "OAUTH_JWKS_ENDPOINT_URI";
    public static final String OAUTH_VALID_ISSUER_URI = PREFIX + "OAUTH_VALID_ISSUER_URI";
    public static final String OAUTH_TOKEN_ENDPOINT_URI = PREFIX + "OAUTH_TOKEN_ENDPOINT_URI";

    public static final String BROKER_TLS_ENABLED = PREFIX + "BROKER_TLS_ENABLED";
    public static final String BROKER_TRUSTED_CERT = PREFIX + "BROKER_TRUSTED_CERT";

    public static final String TLS_CERT = PREFIX + "TLS_CERT";
    public static final String TLS_KEY = PREFIX + "TLS_KEY";
    public static final String TLS_VERSION = PREFIX + "TLS_VERSION";

    public static final String ACL_RESOURCE_OPERATIONS = PREFIX + "ACL_RESOURCE_OPERATIONS";

    private final boolean basicEnabled;
    private final boolean oauthEnabled;
    private final boolean brokerTlsEnabled;
    private final Map<String, Object> acConfig;

    public KafkaAdminConfigRetriever() {
        basicEnabled = System.getenv(BASIC_ENABLED) != null && Boolean.valueOf(System.getenv(BASIC_ENABLED));
        oauthEnabled = System.getenv(OAUTH_ENABLED) == null || Boolean.valueOf(System.getenv(OAUTH_ENABLED));
        brokerTlsEnabled = Boolean.valueOf(System.getenv(BROKER_TLS_ENABLED));
        acConfig = envVarsToAdminClientConfig(PREFIX);
        logConfiguration();
    }

    private Map<String, Object> envVarsToAdminClientConfig(String prefix) {
        Map<String, Object> envConfig = System.getenv().entrySet().stream().filter(entry -> entry.getKey().startsWith(prefix)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Object> adminClientConfig = new HashMap<>();
        if (envConfig.get(BOOTSTRAP_SERVERS) == null) {
            throw new IllegalStateException("Bootstrap address has to be specified");
        }
        adminClientConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envConfig.get(BOOTSTRAP_SERVERS).toString());

        boolean saslEnabled;

        // oAuth
        if (oauthEnabled) {
            log.info("OAuth enabled");
            saslEnabled = true;
            adminClientConfig.put(SaslConfigs.SASL_MECHANISM, OAUTHBEARER);
            adminClientConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        } else if (basicEnabled) {
            log.info("SASL/PLAIN from HTTP Basic authentication enabled");
            saslEnabled = true;
            adminClientConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        } else {
            log.info("Broker authentication/SASL disabled");
            saslEnabled = false;
        }

        StringBuilder protocol = new StringBuilder();

        if (saslEnabled) {
            protocol.append("SASL_");
        }

        if (brokerTlsEnabled) {
            protocol.append(SecurityProtocol.SSL.name);
            String brokerTrustedCert = getBrokerTrustedCertificate();

            if (brokerTrustedCert != null && !brokerTrustedCert.isBlank()) {
                adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, brokerTrustedCert);
                adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            }
        } else {
            protocol.append(SecurityProtocol.PLAINTEXT.name);
        }

        adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

        // admin client
        adminClientConfig.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        adminClientConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");

        return adminClientConfig;
    }

    private void logConfiguration() {
        log.info("AdminClient configuration:");
        acConfig.entrySet().forEach(entry -> {
            log.info("\t{} = {}", entry.getKey(), entry.getValue());
        });
    }

    public boolean isBasicEnabled() {
        return basicEnabled;
    }

    public boolean isOauthEnabled() {
        return oauthEnabled;
    }

    public Map<String, Object> getAcConfig() {
        return new HashMap<>(acConfig);
    }

    public String getBrokerTrustedCertificate() {
        String value = System.getenv(BROKER_TRUSTED_CERT);

        try {
            final Path certPath = Path.of(value);

            if (Files.isReadable(certPath)) {
                return Files.readString(certPath);
            }
        } catch (InvalidPathException e) {
            log.debug("Value of {} was not a valid Path: {}", BROKER_TRUSTED_CERT, e.getMessage());
        } catch (Exception e) {
            log.warn("Exception loading value of {} as a file: {}", BROKER_TRUSTED_CERT, e.getMessage());
        }

        try {
            value = new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
            log.debug("Successfully decoded base-64 cert config value");
        } catch (IllegalArgumentException e) {
            log.debug("Cert config value was not base-64 encoded: {}", e.getMessage());
        }

        return value;
    }

    public String getOauthTrustedCertificate() {
        return System.getenv(OAUTH_TRUSTED_CERT);
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

    public String getAclResourceOperations() {
        String value = System.getenv(ACL_RESOURCE_OPERATIONS);

        return value != null ? value : "{}";
    }
}

