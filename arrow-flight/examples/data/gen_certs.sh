#!/bin/sh

# source: https://users.rust-lang.org/t/use-tokio-tungstenite-with-rustls-instead-of-native-tls-for-secure-websockets/90130

make_and_sign() {
    # Create unencrypted private key and a CSR (certificate signing request)
    openssl req -newkey rsa:2048 -nodes -subj "/C=FI/CN=vahid" -keyout "$1.key" -out "$1.csr"

    # Create self-signed certificate (`$1.pem`) with the private key and CSR
    openssl x509 -signkey "$1.key" -in "$1.csr" -req -days 365 -out "$1.pem"

    # Sign the CSR (`$1.pem`) with the root CA certificate and private key
    # => this overwrites `$1.pem` because it gets signed
    openssl x509 -req -CA ca_root.pem -CAkey ca_root.key -in "$1.csr" -out "$1.pem" -days 1825 -CAcreateserial -extfile localhost.ext
}

# Create a self-signed root CA
openssl req -x509 -sha256 -nodes -subj "/C=FI/CN=vahid" -days 1825 -newkey rsa:2048 -keyout ca_root.key -out ca_root.pem

# Create file localhost.ext with the following content:
cat <<'EOF' > localhost.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
EOF

make_and_sign client
make_and_sign server
