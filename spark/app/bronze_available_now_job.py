from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


BRONZE_PATH = "/opt/spark-data/bronze/transactions"
CHECKPOINT_PATH = "/opt/spark-data/checkpoints/bronze/transactions_available_now"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "transactions"


spark = (
    SparkSession.builder
    .appName("bronze-available-now-job")
    .getOrCreate()
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
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
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

bronze_query.awaitTermination()
