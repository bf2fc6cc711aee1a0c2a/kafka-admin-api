#!/usr/bin/env sh
while getopts e: flag
do
    case "${flag}" in
        e) export ${OPTARG}
          echo ${OPTARG}
          ;;
        *)
          ;;
    esac
done

LOG4J_DEFAULT="${KAFKA_ADMIN_API_HOME}/config/log4j2.properties"
LOG4J_CUSTOM="${KAFKA_ADMIN_API_HOME}/custom-config/log4j2.properties"

LOG4J_CONFIG="file:${LOG4J_DEFAULT},file:${LOG4J_CUSTOM}"
echo "Log4J properties: ${LOG4J_CONFIG}"

export JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configurationFile=${LOG4J_CONFIG}"

exec java ${KAFKA_ADMIN_DEBUG} ${JAVA_OPTS} -cp ./kafka-admin.jar org.bf2.admin.Main -XX:+ExitOnOutOfMemoryError
