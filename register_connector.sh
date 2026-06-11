#!/bin/bash
source .env

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
  "name": "ecommerce-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "'"$POSTGRES_USER"'",
    "database.password": "'"$POSTGRES_PASSWORD"'",
    "database.dbname": "'"$POSTGRES_DB"'",
    "topic.prefix": "ecommerce",
    "schema.include.list": "demo",
    "plugin.name": "pgoutput",
    "snapshot.mode": "never",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table",
    "decimal.handling.mode": "string"
  }
}'
echo -e "\nDebezium Connector registered successfully!"
