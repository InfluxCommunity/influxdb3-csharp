#!/bin/bash
PKCS12_PASSWORD="password12"

echo "### Generate root CA key..."
openssl genrsa -out rootCA.key 2048

echo "### Generate root CA certificate..."
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 3650 -out rootCA.pem -subj "/CN=MyTestRootCA"

echo "### Generate server key..."
openssl genrsa -out server.key 2048

echo "### Generate server certificate request..."
openssl req -new -nodes -newkey rsa:4096 \
  -keyout server.key \
  -out server.csr \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "extendedKeyUsage=serverAuth"

echo "### Sign the request with the root CA..."
openssl x509 -req -in server.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out server.pem -days 365 -sha256 -copy_extensions copy

echo "### Add server certificates into an encrypted PKCS#12 file for WireMock..."
openssl pkcs12 -export -out server.p12 -inkey server.key -in server.pem -certfile rootCA.pem -password pass:${PKCS12_PASSWORD}

echo
echo "### Print rootCA.pem..."
openssl x509 -in rootCA.pem -text -noout

echo
echo "### Print server.pem..."
openssl x509 -in server.pem -text -noout


## Check certificate on MacOS:
# security verify-cert -r rootCA.pem -c server.pem -p ssl -v

echo
echo "### Cleanup..."
rm server.csr
rm rootCA.srl
