#!/bin/bash
set -e

echo "==> Waiting for credentials file..."
until [ -f "$CREDENTIALS_FILE" ]; do
  echo "   waiting for $CREDENTIALS_FILE..."
  sleep 2
done
echo "✅ Credentials file exists"

set -a
. "$CREDENTIALS_FILE"
set +a

if [ -z "$USER_CLIENT_ID" ] || [ -z "$USER_CLIENT_SECRET" ]; then
  echo "❌ Missing USER_CLIENT_ID or USER_CLIENT_SECRET"
  exit 1
fi
echo "✅ Credentials loaded"

CATALOG_TEMPLATE="/etc/trino/catalog/dev_catalog.properties.template"
CATALOG_RENDERED="/etc/trino/catalog/dev_catalog.properties"

echo "==> Rendering catalog config via sed"
sed \
  -e "s|\${POLARIS_HOST}|${POLARIS_HOST}|g" \
  -e "s|\${POLARIS_PORT}|${POLARIS_PORT}|g" \
  -e "s|\${CATALOG_NAME}|${CATALOG_NAME}|g" \
  -e "s|\${USER_CLIENT_ID}|${USER_CLIENT_ID}|g" \
  -e "s|\${USER_CLIENT_SECRET}|${USER_CLIENT_SECRET}|g" \
  -e "s|\${MINIO_HOST}|${MINIO_HOST}|g" \
  -e "s|\${MINIO_PORT}|${MINIO_PORT}|g" \
  -e "s|\${MINIO_ROOT_USER}|${MINIO_ROOT_USER}|g" \
  -e "s|\${MINIO_ROOT_PASSWORD}|${MINIO_ROOT_PASSWORD}|g" \
  "$CATALOG_TEMPLATE" > "$CATALOG_RENDERED"
echo "✅ Catalog config rendered"

exec /usr/lib/trino/bin/run-trino