FROM quay.io/keycloak/keycloak:14.0.0

RUN mkdir /opt/jboss/realms
COPY ${PWD}/docker/keycloak-import/realms/* /opt/jboss/realms/

COPY ${PWD}/docker/target/kafka/config/ca-truststore.p12 /opt/kafka/config/strimzi/
COPY ${PWD}/docker/keycloak-import/start.sh /opt/jboss/

ENV KEYCLOAK_HOST "keycloak"
ENTRYPOINT []
CMD ["/bin/bash", "/opt/jboss/start.sh"]
