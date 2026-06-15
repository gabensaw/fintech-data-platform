from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FintechDataPlatform")
    .getOrCreate()
)

print("Spark started successfully")

spark.stop()