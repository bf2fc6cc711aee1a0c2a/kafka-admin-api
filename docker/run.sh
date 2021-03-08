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
java -cp ./kafka-admin-${KAFKA_ADMIN_API_VERSION}-fat.jar:./health-${KAFKA_ADMIN_API_VERSION}-fat.jar:./rest-${KAFKA_ADMIN_API_VERSION}-fat.jar:./http-server-${KAFKA_ADMIN_API_VERSION}-fat.jar bf.admin.Main -XX:+ExitOnOutOfMemoryError