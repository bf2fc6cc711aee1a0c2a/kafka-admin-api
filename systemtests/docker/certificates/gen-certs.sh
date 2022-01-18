#!/bin/bash

set -e

SCRIPTDIR=$(dirname "$0")

rm -rvf ./certs
mkdir -p ./certs
cd ./certs

${SCRIPTDIR}/gen-ca.sh
${SCRIPTDIR}/gen-keycloak-certs.sh
${SCRIPTDIR}/gen-kafka-certs.sh
${SCRIPTDIR}/gen-admin-certs.sh
