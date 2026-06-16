from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, lit
from schemas import transaction_schema


spark = (
    SparkSession.builder
    .appName("silver-job")
    .getOrCreate()
)

bronze_df = spark.read.parquet("/opt/spark-data/bronze/transactions")

parsed_df = (
    bronze_df
    .select(
        from_json(
            col("raw_event_json"),
            transaction_schema
        ).alias("data"),
        col("kafka_timestamp")
    )
    .filter(col("data").isNotNull())
)

# flatten the struct
silver_df = (
    parsed_df.select("data.*", "kafka_timestamp")
)

# type conversions
silver_df = (
    silver_df
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
)

# metadata
silver_df = (
    silver_df
    .withColumn("silver_ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_version", lit("v1.0"))
)

#data quality checks
silver_df = (
    silver_df
    .filter(col("transaction_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("merchant").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("currency").isNotNull())
    .filter(col("country").isNotNull())
    .filter(col("payment_method").isNotNull())
    .filter(col("fraud_flag").isNotNull())
    .filter(col("event_timestamp").isNotNull())
)

#save to parquet
silver_df.cache()  # Cache the DataFrame to avoid recomputation

silver_df.write.mode("overwrite").parquet("/opt/spark-data/silver/transactions")

print("\n===============================")
print(f"Silver records: {silver_df.count()}")
print("===============================\n")

input_count = parsed_df.count()
output_count = silver_df.count()

print("\n===============================")
print(f"Input records: {input_count}")
print(f"Output records: {output_count}")
print(f"Rejected records: {input_count - output_count}")
print("===============================\n")


# TODO:
# replace overwrite with incremental load
# when Airflow orchestration is added

# TODO:
# dropDuplicates(["transaction_id"])
# after watermark implementation