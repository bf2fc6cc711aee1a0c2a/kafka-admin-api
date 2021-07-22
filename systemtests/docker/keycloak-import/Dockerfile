FROM quay.io/keycloak/keycloak:14.0.0

ENV KEYCLOAK_HOST "keycloak"
ENTRYPOINT []
CMD ["/bin/bash", "/opt/jboss/start.sh"]

COPY ${PWD}/docker/keycloak-import/realms/  /opt/jboss/realms/
COPY ${PWD}/docker/keycloak-import/start.sh /opt/jboss/
