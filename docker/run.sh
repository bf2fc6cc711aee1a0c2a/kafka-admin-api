#!/usr/bin/env sh

java -cp ./kafka-admin-${KAFKA_ADMIN_API_VERSION}-fat.jar:./health-${KAFKA_ADMIN_API_VERSION}-fat.jar:./rest-${KAFKA_ADMIN_API_VERSION}-fat.jar:./http-server-${KAFKA_ADMIN_API_VERSION}-fat.jar bf.admin.Main -XX:+ExitOnOutOfMemoryError