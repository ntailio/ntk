#!/bin/sh
set -eu
cd /certs

if [ -f ca.crt ]; then
  echo "certs: already present, skipping"
  exit 0
fi

DAYS=3650

# ca.key is kept so tests can mint client certs for random principals.
openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" \
  -subj "/CN=ntk-sandbox-ca" -keyout ca.key -out ca.crt

for n in 1 2 3; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=kafka-$n" \
    -keyout "kafka-$n.plain.key" -out "kafka-$n.csr"
  printf 'subjectAltName=DNS:kafka-%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' "$n" > ext.cnf
  openssl x509 -req -in "kafka-$n.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -extfile ext.cnf -out "kafka-$n.crt"
  openssl pkcs8 -topk8 -v2 aes-256-cbc -in "kafka-$n.plain.key" \
    -passout pass:broker-secret -out "kafka-$n.key"
  cat "kafka-$n.key" "kafka-$n.crt" ca.crt > "kafka-$n.keystore.pem"
  rm "kafka-$n.plain.key" "kafka-$n.csr"
done

for u in admin bob; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$u" -keyout "$u.key" -out "$u.csr"
  printf 'extendedKeyUsage=clientAuth\n' > ext.cnf
  openssl x509 -req -in "$u.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -extfile ext.cnf -out "$u.crt"
  rm "$u.csr"
done

rm -f ext.cnf ca.srl

chmod 644 ./*
chown "$(stat -c %u:%g .)" ./*
echo "certs: generated"
