#!/usr/bin/env bash
# Generate a self-signed dev certificate for lesquic (localhost).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p certs
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout certs/key.pem -out certs/cert.pem \
  -days 3650 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" 2>/dev/null
echo "certs/cert.pem + certs/key.pem generated"
