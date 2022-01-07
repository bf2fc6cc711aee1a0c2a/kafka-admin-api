FROM registry.access.redhat.com/ubi8/openjdk-11-runtime

USER root
# Switch to legacy crypto policy and disable the TLS 3DES cipher suites
RUN update-crypto-policies --set LEGACY && \
    sed -i 's/jdk.tls.disabledAlgorithms=/jdk.tls.disabledAlgorithms=3DES_EDE_CBC, /' /usr/share/crypto-policies/LEGACY/java.txt
USER 185

ENV KAFKA_ADMIN_API_HOME=/opt/kafka-admin-api
WORKDIR ${KAFKA_ADMIN_API_HOME}

CMD [ "./run.sh" ]

COPY ./src/main/docker/resources/ ${KAFKA_ADMIN_API_HOME}/
COPY ./target/lib/                ${KAFKA_ADMIN_API_HOME}/lib/
COPY ./target/kafka-admin.jar     ${KAFKA_ADMIN_API_HOME}/
