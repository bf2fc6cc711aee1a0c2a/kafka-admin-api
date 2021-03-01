FROM quay.io/strimzi/kafka:0.21.1-kafka-2.6.0
COPY ${PWD}/docker/target/kafka/libs/* /opt/kafka/libs/strimzi/
COPY ${PWD}/docker/target/kafka/config/* /opt/kafka/config/strimzi/
COPY ${PWD}/docker/kafka/scripts /opt/kafka/
COPY ${PWD}/docker/target/kafka/certs/*.p12 /tmp/kafka/



ENV LOG_DIR: /home/kafka/logs
      #KAFKA_LOG_DIRS: /home/kafka/1
      # Java Debug
      #KAFKA_DEBUG: y
      #DEBUG_SUSPEND_FLAG: y
      #JAVA_DEBUG_PORT: 5006

      #
      # KAFKA Configuration
      #
ENV LOG_DIR /home/kafka/logs

ENV KAFKA_BROKER_ID 1
ENV KAFKA_ZOOKEEPER_CONNECT zookeeper:2181
ENV KAFKA_LISTENERS REPLICATION://kafka:9091,CLIENT://kafka:9092
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP REPLICATION:SSL,CLIENT:SASL_PLAINTEXT
ENV KAFKA_SASL_ENABLED_MECHANISMS OAUTHBEARER
ENV KAFKA_INTER_BROKER_LISTENER_NAME REPLICATION
ENV KAFKA_SSL_SECURE_RANDOM_IMPLEMENTATION SHA1PRNG
ENV KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM ""

ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_KEYSTORE_LOCATION /tmp/kafka/cluster.keystore.p12
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_KEYSTORE_PASSWORD Z_pkTh9xgZovK4t34cGB2o6afT4zZg0L
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_KEYSTORE_TYPE PKCS12
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_TRUSTSTORE_LOCATION /tmp/kafka/cluster.truststore.p12
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_TRUSTSTORE_PASSWORD Z_pkTh9xgZovK4t34cGB2o6afT4zZg0L
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_TRUSTSTORE_TYPE PKCS12
ENV KAFKA_LISTENER_NAME_REPLICATION_SSL_CLIENT_AUTH required

ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_JAAS_CONFIG "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_LOGIN_CALLBACK_HANDLER_CLASS io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler

ENV KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR 1

ENV KAFKA_AUTHORIZER_CLASS_NAME io.strimzi.kafka.oauth.server.OAuthSessionAuthorizer
ENV KAFKA_PRINCIPAL_BUILDER_CLASS io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder
ENV KAFKA_STRIMZI_AUTHORIZER_DELEGATE_CLASS_NAME io.strimzi.kafka.oauth.server.authorizer.KeycloakRBACAuthorizer

ENV KAFKA_STRIMZI_AUTHORIZATION_KAFKA_CLUSTER_NAME cluster2
ENV KAFKA_STRIMZI_AUTHORIZATION_DELEGATE_TO_KAFKA_ACL "true"
ENV KAFKA_STRIMZI_AUTHORIZATION_GRANTS_REFRESH_POOL_SIZE "10"
ENV KAFKA_STRIMZI_AUTHORIZATION_GRANTS_REFRESH_PERIOD_SECONDS "30"
ENV KAFKA_SUPER_USERS User:CN=my-cluster-kafka,O=io.strimzi;User:CN=my-cluster-entity-operator,O=io.strimzi;User:CN=my-cluster-kafka-exporter,O=io.strimzi;User:service-account-kafka

ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_CONNECTIONS_MAX_REAUTH_MS 3600000

      #
      # Strimzi OAuth Configuration
      #

      # Authentication config
ENV OAUTH_CLIENT_ID "kafka"
ENV OAUTH_CLIENT_SECRET "kafka-secret"
ENV OAUTH_TOKEN_ENDPOINT_URI "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/token"

      # Validation config
ENV OAUTH_VALID_ISSUER_URI "http://keycloak:8080/auth/realms/kafka-authz"
ENV OAUTH_JWKS_ENDPOINT_URI "http://keycloak:8080/auth/realms/kafka-authz/protocol/openid-connect/certs"
      #OAUTH_INTROSPECTION_ENDPOINT_URI: "http://${KEYCLOAK_HOST:-keycloak}:8080/auth/realms/${REALM:-demo}/protocol/openid-connect/token/introspect"

      # username extraction from JWT token claim
ENV OAUTH_USERNAME_CLAIM preferred_username

ENV OAUTH_JWKS_REFRESH_MIN_PAUSE_SECONDS "5"

      # For start.sh script to know where the keycloak is listening
ENV KEYCLOAK_HOST: keycloak
ENV REALM kafka-authz

CMD ["/bin/bash", "/opt/kafka/start.sh"]


