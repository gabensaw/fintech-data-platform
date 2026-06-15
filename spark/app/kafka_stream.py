from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaStreamingTest")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .load()
)

query = (
    df.selectExpr(
        "CAST(value AS STRING)"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()