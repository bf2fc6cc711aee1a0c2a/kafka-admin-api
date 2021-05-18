#!/bin/sh

set -e

CA_KEY=ca.key
CA_CERT=ca.crt

ADMIN_KEY=admin-tls.key
ADMIN_CSR=admin-tls.csr
ADMIN_CERT=admin-tls.crt

if ! [ -f "${CA_CERT}" ] ; then
    ./gen-ca.sh
fi

echo "#### Generate Kafka Admin Server RSA Key"
openssl genrsa -out ${ADMIN_KEY} 4096

echo "#### Generate Kafka Admin Server certificate request"
openssl req -new -key ${ADMIN_KEY} -out ${ADMIN_CSR} -subj '/CN=localhost'

echo "#### Sign the certificate with the CA"
openssl x509 -req -CA ${CA_CERT} -CAkey ${CA_KEY} -in ${ADMIN_CSR} -out ${ADMIN_CERT} -days 400 -CAcreateserial -passin pass:$STOREPASS

echo "#### Create certificate chain with ${ADMIN_CERT} and ${CA_CERT}"
cat ${ADMIN_CERT} ${CA_CERT} > admin-tls-chain.crt
