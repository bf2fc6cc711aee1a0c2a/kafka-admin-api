FROM registry.access.redhat.com/ubi8/openjdk-11:1.3

ARG kafka_admin_api_version=0.0.7
ENV KAFKA_ADMIN_API_VERSION ${kafka_admin_api_version}

COPY health/target/health-${kafka_admin_api_version}-fat.jar \
     rest/target/rest-${kafka_admin_api_version}-fat.jar \
     http-server/target/http-server-${kafka_admin_api_version}-fat.jar \
     kafka-admin/target/kafka-admin-${kafka_admin_api_version}-fat.jar \
     docker/run.sh ./

CMD [ "./run.sh"]
