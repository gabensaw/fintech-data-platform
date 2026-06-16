from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# --------------------------------
# Spark Session
# --------------------------------

spark = (
    SparkSession.builder
    .appName("bronze-stream")
    .getOrCreate()
)

# --------------------------------
# Kafka Source
# --------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .load()
)

bronze_df = (
    kafka_df.selectExpr(
        "CAST(value AS STRING) as raw_event_json",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )
    .withColumn("bronze_ingestion_timestamp", current_timestamp())
)

bronze_query = (
    bronze_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/spark-data/bronze/transactions")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/bronze/transactions")
    .start()
)

bronze_query.awaitTermination()