FROM quay.io/keycloak/keycloak:14.0.0

COPY ${PWD}/docker/keycloak/scripts/* /opt/jboss/keycloak/
COPY ${PWD}/docker/target/keycloak/certs/* /opt/jboss/keycloak/standalone/configuration/certs/

ENV KEYCLOAK_USER "admin"
ENV KEYCLOAK_PASSWORD "admin"
ENV PROXY_ADDRESS_FORWARDING "true"
CMD ["-Dkeycloak.profile.feature.upload_scripts=enabled"]
