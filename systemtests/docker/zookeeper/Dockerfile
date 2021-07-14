FROM quay.io/strimzi/kafka:0.23.0-kafka-2.7.0

ADD ${PWD}/docker/zookeeper/scripts /opt/kafka/strimzi

ENV LOG_DIR /tmp/logs

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["cd /opt/kafka/strimzi && ./start.sh"]
