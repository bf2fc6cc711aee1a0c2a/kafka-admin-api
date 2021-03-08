FROM quay.io/strimzi/kafka:0.21.1-kafka-2.6.0
ADD ${PWD}/docker/zookeeper/scripts /opt/kafka/strimzi

ENV LOG_DIR /tmp/logs

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["cd /opt/kafka/strimzi && ./start.sh"]