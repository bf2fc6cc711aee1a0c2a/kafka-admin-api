FROM centos:7
ARG JAVA_VERSION=11

RUN yum -y update \
    && yum -y install java-${JAVA_VERSION}-openjdk-headless openssl \
    && yum -y clean all

#####
# Add Tini
#####
ENV TINI_VERSION v0.18.0
ENV TINI_SHA256=12d20136605531b09a2c2dac02ccee85e1b874eb322ef6baf7561cd93f93c855
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN echo "${TINI_SHA256} */usr/bin/tini" | sha256sum -c \
    && chmod +x /usr/bin/tini

RUN useradd -r -m -u 1001 -g 0 user

ARG kafka_admin_api_version=1.0-SNAPSHOT
ENV KAFKA_ADMIN_API_VERSION ${kafka_admin_api_version}
ENV KAFKA_ADMIN_HOME=/opt/kafka_admin
RUN mkdir -p ${KAFKA_ADMIN_HOME}
WORKDIR ${KAFKA_ADMIN_HOME}

COPY health/target/health-${kafka_admin_api_version}-fat.jar ./
COPY rest/target/rest-${kafka_admin_api_version}-fat.jar ./
COPY http-server/target/http-server-${kafka_admin_api_version}-fat.jar ./
COPY kafka-admin/target/kafka-admin-${kafka_admin_api_version}-fat.jar ./
COPY docker/run.sh ./


ENTRYPOINT ["/usr/bin/tini", "-w", "-e", "143", "--", "sh", "-c", "/opt/kafka_admin/run.sh "]