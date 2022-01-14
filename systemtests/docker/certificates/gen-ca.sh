#!/bin/sh
set -e

# create CA key
openssl genrsa -out ca.key 4096

# create CA certificate
openssl req -x509 -new -nodes -sha256 -days 3650 -subj "/CN=KafkaAdmin.io" -key ca.key -out ca.crt
