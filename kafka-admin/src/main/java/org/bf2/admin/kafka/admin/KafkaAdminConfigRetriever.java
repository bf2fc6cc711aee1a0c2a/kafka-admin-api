package org.bf2.admin.kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * KafkaAdminConfigRetriever class gets configuration from envvars
 */
@Singleton
public class KafkaAdminConfigRetriever {

    protected final Logger log = LogManager.getLogger(KafkaAdminConfigRetriever.class);

    private static final String PREFIX = "kafka.admin.";
    private static final String OAUTHBEARER = "OAUTHBEARER";

    public static final String BOOTSTRAP_SERVERS = PREFIX + "bootstrap.servers";
    public static final String API_TIMEOUT_MS_CONFIG = PREFIX + "api.timeout.ms.config";
    public static final String REQUEST_TIMEOUT_MS_CONFIG = PREFIX + "request.timeout.ms.config";
    public static final String BASIC_ENABLED = PREFIX + "basic.enabled";
    public static final String OAUTH_ENABLED = PREFIX + "oauth.enabled";
    // TODO: PR to allow configuration via smallrye-jwt
    public static final String OAUTH_TRUSTED_CERT = PREFIX + "oauth.trusted.cert";
    public static final String OAUTH_JWKS_ENDPOINT_URI = PREFIX + "oauth.jwks.endpoint.uri";
    public static final String OAUTH_TOKEN_ENDPOINT_URI = PREFIX + "oauth.token.endpoint.uri";

    public static final String BROKER_TLS_ENABLED = PREFIX + "broker.tls.enabled";
    public static final String BROKER_TRUSTED_CERT = PREFIX + "broker.trusted.cert";

    public static final String ACL_RESOURCE_OPERATIONS = PREFIX + "acl.resource.operations";

    @Inject
    @ConfigProperty(name = BOOTSTRAP_SERVERS)
    String bootstrapServers;

    @Inject
    @ConfigProperty(name = API_TIMEOUT_MS_CONFIG, defaultValue = "30000")
    String apiTimeoutMsConfig;

    @Inject
    @ConfigProperty(name = REQUEST_TIMEOUT_MS_CONFIG, defaultValue = "10000")
    String requestTimeoutMsConfig;

    @Inject
    @ConfigProperty(name = BASIC_ENABLED, defaultValue = "false")
    boolean basicEnabled;

    @Inject
    @ConfigProperty(name = OAUTH_ENABLED, defaultValue = "true")
    boolean oauthEnabled;

    @Inject
    @ConfigProperty(name = BROKER_TLS_ENABLED, defaultValue = "false")
    boolean brokerTlsEnabled;

    @Inject
    @ConfigProperty(name = BROKER_TRUSTED_CERT)
    Optional<String> brokerTrustedCert;

    @Inject
    @ConfigProperty(name = ACL_RESOURCE_OPERATIONS, defaultValue = "{}")
    String aclResourceOperations;

    Map<String, Object> acConfig;

    @PostConstruct
    public void initialize() {
        acConfig = envVarsToAdminClientConfig();
        logConfiguration();
    }

    private Map<String, Object> envVarsToAdminClientConfig() {
        Map<String, Object> adminClientConfig = new HashMap<>();

        adminClientConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        boolean saslEnabled;

        // oAuth
        if (oauthEnabled) {
            log.info("OAuth enabled");
            saslEnabled = true;
            adminClientConfig.put(SaslConfigs.SASL_MECHANISM, OAUTHBEARER);
            adminClientConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            // Do not attempt token refresh ahead of expiration (ExpiringCredentialRefreshingLogin)
            // May still cause warnings to be logged when token will expired in less than SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS.
            adminClientConfig.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, "0");
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

            brokerTrustedCert.ifPresent(certConfig -> {
                String certContent = getBrokerTrustedCertificate(certConfig);

                if (certContent != null && !certContent.isBlank()) {
                    adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, certContent);
                    adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
                }
            });
        } else {
            protocol.append(SecurityProtocol.PLAINTEXT.name);
        }

        adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

        // admin client
        adminClientConfig.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        adminClientConfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMsConfig);
        adminClientConfig.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, apiTimeoutMsConfig);

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

    public String getBrokerTrustedCertificate(final String certConfig) {
        try {
            final Path certPath = Path.of(certConfig);

            if (Files.isReadable(certPath)) {
                return Files.readString(certPath);
            }
        } catch (InvalidPathException e) {
            log.debug("Value of {} was not a valid Path: {}", BROKER_TRUSTED_CERT, e.getMessage());
        } catch (Exception e) {
            log.warn("Exception loading value of {} as a file: {}", BROKER_TRUSTED_CERT, e.getMessage());
        }

        String value = null;

        try {
            value = new String(Base64.getDecoder().decode(certConfig), StandardCharsets.UTF_8);
            log.debug("Successfully decoded base-64 cert config value");
        } catch (IllegalArgumentException e) {
            log.debug("Cert config value was not base-64 encoded: {}", e.getMessage());
        }

        return value;
    }

    public String getAclResourceOperations() {
        return aclResourceOperations;
    }
}

