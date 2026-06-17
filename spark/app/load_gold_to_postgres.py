from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("load-gold-to-postgres")
    .getOrCreate()
)

POSTGRES_URL = "jdbc:postgresql://postgres:5432/fintech"

POSTGRES_PROPERTIES = {
    "user": "fintech",
    "password": "fintech",
    "driver": "org.postgresql.Driver"
}

# daily metrics
daily_metrics_df = spark.read.parquet(
    "/opt/spark-data/gold/daily_transaction_metrics"
)

daily_metrics_df.write \
    .option("truncate", "true") \
    .mode("overwrite") \
    .jdbc(
        url=POSTGRES_URL,
        table="daily_transaction_metrics",
        properties=POSTGRES_PROPERTIES
    )

# merchant metrics
merchant_metrics_df = spark.read.parquet(
    "/opt/spark-data/gold/merchant_metrics"
)

merchant_metrics_df.write \
    .option("truncate", "true") \
    .mode("overwrite") \
    .jdbc(
        url=POSTGRES_URL,
        table="merchant_metrics",
        properties=POSTGRES_PROPERTIES
    )

print("Gold datasets loaded to PostgreSQL")