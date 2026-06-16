from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum, avg, when, round


spark = (
    SparkSession.builder
    .appName("gold-job")
    .getOrCreate()
)

silver_df = spark.read.parquet("/opt/spark-data/silver/transactions")

silver_df = (
    silver_df
    .withColumn(
        "transaction_date", 
        to_date(col("event_timestamp"))
    )
)

daily_metrics_df = (
    silver_df.groupBy("transaction_date")
    .agg(
        count("*").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        round(avg("amount"), 2).alias("average_amount"),
        sum(
            when(col("fraud_flag"), 1).otherwise(0)
        ).alias("fraud_count")
    )
)

# fraud rate on daily level
daily_metrics_df = (
    daily_metrics_df
    .withColumn(
        "fraud_rate", 
        round(col("fraud_count") / col("total_transactions"), 4)
    )
)

merchant_metrics_df = (
    silver_df.groupBy("merchant")
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        round(avg("amount"), 2).alias("average_amount"),
        sum(
            when(col("fraud_flag"), 1).otherwise(0)
        ).alias("fraud_count")
    )
)

# save gold metrics to parquet
daily_metrics_df.write.mode("overwrite").parquet("/opt/spark-data/gold/daily_transaction_metrics")
merchant_metrics_df.write.mode("overwrite").parquet("/opt/spark-data/gold/merchant_metrics")


daily_metrics_df.show(truncate=False) # DEV debug 
merchant_metrics_df.show(truncate=False)  # DEV debug 

