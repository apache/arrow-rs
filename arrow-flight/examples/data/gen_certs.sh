#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Generate test-only certificates with OpenSSL 1.1.1 or later.
# Run from arrow-flight/examples/data: sh gen_certs.sh
# Overwrites ca_root, client and server .pem/.key files in the current directory.
set -eu

cert_tmp=$(mktemp -d)
trap 'rm -rf "$cert_tmp"' EXIT
trap 'exit 1' HUP INT TERM

openssl req -new -x509 -newkey rsa:2048 -nodes -sha256 -days 1825 \
    -config /dev/null -subj "/CN=Arrow Flight Test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash" \
    -keyout ca_root.key -out ca_root.pem

for identity in client server; do
    openssl req -new -newkey rsa:2048 -nodes -sha256 \
        -config /dev/null -subj "/CN=Arrow Flight Test $identity" \
        -keyout "$identity.key" -out "$cert_tmp/request.csr"

    cat > "$cert_tmp/extensions.cnf" <<EOF
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
