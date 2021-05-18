FROM registry.access.redhat.com/ubi8/openjdk-11:1.3-15

ENV KAFKA_ADMIN_API_HOME=/opt/kafka-admin-api
WORKDIR ${KAFKA_ADMIN_API_HOME}

CMD [ "./run.sh" ]

COPY docker/run.sh                      ./
COPY docker/log4j2.properties           ./config/
COPY kafka-admin/target/lib/            ./lib/
COPY kafka-admin/target/kafka-admin.jar ./
