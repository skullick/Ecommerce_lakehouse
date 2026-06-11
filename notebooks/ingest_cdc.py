import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

def get_schema(table_name):
    """
    Returns the raw source schema for the specific table.
    In a Medallion Architecture, Bronze should reflect the source as closely as possible.
    """
    base_fields = [
        StructField("__op", StringType()),
        StructField("__table", StringType())
    ]
    
    schemas = {
        "customer": [
            StructField("customer_id", StringType()),
            StructField("customer_name", StringType()),
            StructField("customer_email", StringType()),
            StructField("customer_mobile", StringType()),
            StructField("customer_birthday", StringType()),
            StructField("customer_gender", StringType()),
            StructField("customer_address_id", StringType()),
            StructField("is_active", BooleanType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ],
        "order": [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_status_id", StringType()),
            StructField("order_date", StringType()),
            StructField("total_amount", StringType()),
            StructField("shipping_address_id", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ],
        "product": [
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("brand_id", StringType()),
            StructField("category_id", StringType()),
            StructField("price", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ]
    }
    
    if table_name not in schemas:
        raise ValueError(f"No schema defined for table: {table_name}")
        
    return StructType(schemas[table_name] + base_fields)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Table name to ingest")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--trigger-once", action="store_true", help="Use Trigger.AvailableNow")
    args = parser.parse_args()

    table_name = args.table
    topic_name = args.topic
    
    spark = SparkSession.builder \
        .appName(f"Bronze-Ingestion-{table_name}") \
        .getOrCreate()

    schema = get_schema(table_name)

    # 1. Read from Kafka with Metadata
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Extract Raw Data + Audit Metadata
    # In Bronze, we keep EVERYTHING from the source + technical metadata
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp")
    ).select(
        "data.*",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        current_timestamp().alias("_ingested_at"),
        lit("bronze").alias("_layer")
    )

    def write_to_bronze(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Ensure the bronze namespace exists
        # Since dev_catalog is the default, we can just use 'bronze'
        spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
        
        target_table = f"bronze.{table_name}"
        
        print(f"==> Ingesting batch {batch_id} to {target_table}...")
        
        # In Bronze, we use APPEND-ONLY logic to preserve history of all changes
        # Use saveAsTable to allow Spark/Iceberg to create the table if it doesn't exist
        batch_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(target_table)

    # 3. Execution Logic
    writer = parsed_df.writeStream \
        .foreachBatch(write_to_bronze) \
        .option("checkpointLocation", f"/tmp/checkpoints/bronze_{table_name}_ingestion")

    if args.trigger_once:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime='30 seconds').start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
