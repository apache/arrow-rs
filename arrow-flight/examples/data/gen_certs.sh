#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Generate test-only certificates with OpenSSL 1.1.1 or later.
# Run from arrow-flight/examples/data: sh gen_certs.sh
# Overwrites ca_root, client and server .pem/.key files in the current directory.
set -eu

cert_tmp=$(mktemp -d)
trap 'rm -rf "$cert_tmp"' EXIT
trap 'exit 1' HUP INT TERM
printf '[req]\ndistinguished_name = dn\n[dn]\n' >"$cert_tmp/req.cnf"

openssl req -new -x509 -newkey rsa:2048 -nodes -sha256 -days 1825 \
  -config "$cert_tmp/req.cnf" -subj "/CN=Arrow Flight Test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -keyout ca_root.key -out ca_root.pem

for identity in client server; do
  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -config "$cert_tmp/req.cnf" -subj "/CN=Arrow Flight Test $identity" \
    -keyout "$identity.key" -out "$cert_tmp/request.csr"

  cat >"$cert_tmp/extensions.cnf" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=${identity}Auth
subjectAltName=DNS:localhost
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
EOF

  openssl x509 -req -in "$cert_tmp/request.csr" -sha256 -days 1825 \
    -CA ca_root.pem -CAkey ca_root.key \
    -CAserial "$cert_tmp/ca.srl" -CAcreateserial \
    -extfile "$cert_tmp/extensions.cnf" -out "$identity.pem"
done
