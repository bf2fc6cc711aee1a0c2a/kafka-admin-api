FROM quay.io/keycloak/keycloak:14.0.0

ENV KEYCLOAK_USER="admin" \
    KEYCLOAK_PASSWORD="admin" \
    PROXY_ADDRESS_FORWARDING="true"

CMD ["-Dkeycloak.profile.feature.upload_scripts=enabled"]

COPY ${PWD}/docker/keycloak/scripts/*                        /opt/jboss/keycloak/
COPY ${PWD}/docker/certificates/keycloak.server.keystore.p12 /opt/jboss/keycloak/standalone/configuration/certs/
