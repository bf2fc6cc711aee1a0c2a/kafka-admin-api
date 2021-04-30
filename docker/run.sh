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

exec java ${KAFKA_ADMIN_DEBUG} -cp ./kafka-admin.jar org.bf2.admin.Main -XX:+ExitOnOutOfMemoryError
