from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    expr,
    from_json,
    lit,
    when,
)

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
        col("raw_event_json"),
        from_json(
            col("raw_event_json"),
            transaction_schema
        ).alias("data"),
        col("kafka_timestamp")
    )
)

parsed_valid_df = (
    parsed_df
    .filter(col("data").isNotNull())
    .select("data.*", "kafka_timestamp", "raw_event_json")
)

silver_candidate_df = (
    parsed_valid_df
    .withColumn("event_timestamp", expr("try_cast(event_timestamp as timestamp)"))
    .withColumn("silver_ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_version", lit("v1.0"))
)

rejection_reason = concat_ws(
    ", ",
    when(col("transaction_id").isNull(), "missing_transaction_id"),
    when(col("customer_id").isNull(), "missing_customer_id"),
    when(col("merchant").isNull(), "missing_merchant"),
    when(col("amount").isNull(), "missing_amount"),
    when(col("amount") <= 0, "non_positive_amount"),
    when(col("currency").isNull(), "missing_currency"),
    when(col("country").isNull(), "missing_country"),
    when(col("payment_method").isNull(), "missing_payment_method"),
    when(col("fraud_flag").isNull(), "missing_fraud_flag"),
    when(col("event_timestamp").isNull(), "invalid_event_timestamp"),
)

validated_df = silver_candidate_df.withColumn(
    "rejection_reason",
    rejection_reason
)

rejected_parse_df = (
    parsed_df
    .filter(col("data").isNull())
    .select(
        col("raw_event_json"),
        col("kafka_timestamp"),
        lit("invalid_json_or_schema").alias("rejection_reason"),
        current_timestamp().alias("rejected_at"),
        lit("v1.0").alias("pipeline_version")
    )
)

rejected_validation_df = (
    validated_df
    .filter(col("rejection_reason") != "")
    .select(
        col("raw_event_json"),
        col("kafka_timestamp"),
        col("rejection_reason"),
        current_timestamp().alias("rejected_at"),
        col("pipeline_version")
    )
)

rejected_df = rejected_parse_df.unionByName(rejected_validation_df)

valid_silver_df = (
    validated_df
    .filter(col("rejection_reason") == "")
    .drop("raw_event_json", "rejection_reason")
)

valid_silver_df.cache()
rejected_df.cache()

valid_silver_df.write.mode("overwrite").parquet(
    "/opt/spark-data/silver/transactions"
)

rejected_df.write.mode("overwrite").parquet(
    "/opt/spark-data/silver/rejected_transactions"
)

input_count = parsed_df.count()
output_count = valid_silver_df.count()
rejected_count = rejected_df.count()

print("\n===============================")
print(f"Input records: {input_count}")
print(f"Silver records: {output_count}")
print(f"Rejected records: {rejected_count}")
print("===============================\n")
