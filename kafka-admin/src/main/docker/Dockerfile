FROM registry.access.redhat.com/ubi8/openjdk-11:1.3-18

ENV KAFKA_ADMIN_API_HOME=/opt/kafka-admin-api
WORKDIR ${KAFKA_ADMIN_API_HOME}

CMD [ "./run.sh" ]

COPY ./src/main/docker/resources/ ${KAFKA_ADMIN_API_HOME}/
COPY ./target/lib/                ${KAFKA_ADMIN_API_HOME}/lib/
COPY ./target/kafka-admin.jar     ${KAFKA_ADMIN_API_HOME}/
