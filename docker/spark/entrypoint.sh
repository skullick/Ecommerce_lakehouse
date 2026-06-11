#!/bin/bash
set -e

echo "==> Waiting for credentials file..."
until [ -f "$CREDENTIALS_FILE" ]; do
  echo "   waiting for $CREDENTIALS_FILE..."
  sleep 2
done
echo "✅ Credentials file exists"

echo "Reading Polaris credentials from $CREDENTIALS_FILE..."
set -a
. "$CREDENTIALS_FILE"
set +a

if [ -z "$USER_CLIENT_ID" ] || [ -z "$USER_CLIENT_SECRET" ]; then
  echo "❌ Missing USER_CLIENT_ID or USER_CLIENT_SECRET"
  exit 1
fi
echo "✅ Credentials loaded"

SPARK_HOME=/usr/local/spark
CONF_TEMPLATE="$SPARK_HOME/conf/spark-defaults.conf.template"
CONF_RENDERED="$SPARK_HOME/conf/spark-defaults.conf"

echo "==> Rendering spark-defaults.conf via envsubst..."
envsubst < "$CONF_TEMPLATE" > "$CONF_RENDERED"
echo "✅ spark-defaults.conf rendered"

exec start-notebook.py \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.open_browser=False \
  --ServerApp.ip='0.0.0.0' \
  --ServerApp.port=8888 \
  --ServerApp.root_dir='/home/jovyan/work'