FROM quay.io/strimzi/kafka:0.21.1-kafka-2.6.0
COPY ${PWD}/docker/target/kafka/libs/* /opt/kafka/libs/strimzi/
COPY ${PWD}/docker/target/kafka/config/* /opt/kafka/config/strimzi/
COPY ${PWD}/docker/kafka/scripts /opt/kafka/
COPY ${PWD}/docker/target/kafka/certs/* /tmp/kafka



ENV LOG_DIR: /home/kafka/logs
      #KAFKA_LOG_DIRS: /home/kafka/1

ENV KAFKA_BROKER_ID 1
ENV KAFKA_ZOOKEEPER_CONNECT zookeeper:2181
ENV KAFKA_LISTENERS CLIENT://kafka:9092
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP CLIENT:SASL_PLAINTEXT
ENV KAFKA_SASL_ENABLED_MECHANISMS OAUTHBEARER
ENV KAFKA_INTER_BROKER_LISTENER_NAME CLIENT
ENV KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL OAUTHBEARER

ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_JAAS_CONFIG "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_LOGIN_CALLBACK_HANDLER_CLASS io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
ENV KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler

ENV KAFKA_SUPER_USERS User:service-account-kafka-broker

ENV KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR 1

ENV OAUTH_CLIENT_ID "kafka-broker"
ENV OAUTH_CLIENT_SECRET "kafka-broker-secret"
ENV OAUTH_TOKEN_ENDPOINT_URI "http://keycloak:8080/auth/realms/demo/protocol/openid-connect/token"

      # Validation config
ENV OAUTH_VALID_ISSUER_URI "http://keycloak:8080/auth/realms/demo"
ENV OAUTH_JWKS_ENDPOINT_URI "http://keycloak:8080/auth/realms/demo/protocol/openid-connect/certs"
      #OAUTH_INTROSPECTION_ENDPOINT_URI: "http://${KEYCLOAK_HOST:-keycloak}:8080/auth/realms/${REALM:-demo}/protocol/openid-connect/token/introspect"


ENV OAUTH_USERNAME_CLAIM preferred_username


ENV KEYCLOAK_HOST keycloak
ENV REALM demo

CMD ["/bin/bash", "/opt/kafka/start.sh"]


