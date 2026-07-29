from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

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


def execute_postgres_sql(sql):
    jvm = spark._sc._gateway.jvm
    class_loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver_class = class_loader.loadClass(POSTGRES_PROPERTIES["driver"])
    driver = driver_class.newInstance()

    connection_properties = jvm.java.util.Properties()
    connection_properties.setProperty("user", POSTGRES_PROPERTIES["user"])
    connection_properties.setProperty("password", POSTGRES_PROPERTIES["password"])

    connection = driver.connect(POSTGRES_URL, connection_properties)

    try:
        statement = connection.createStatement()
        statement.execute(sql)
        statement.close()
    finally:
        connection.close()


# daily metrics
daily_metrics_df = spark.read.parquet(
    "/opt/spark-data/gold/daily_transaction_metrics"
)

daily_metrics_df = daily_metrics_df.withColumn(
    "warehouse_loaded_at",
    current_timestamp()
)

execute_postgres_sql(
    "ALTER TABLE IF EXISTS daily_transaction_metrics "
    "ADD COLUMN IF NOT EXISTS warehouse_loaded_at TIMESTAMP"
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

merchant_metrics_df = merchant_metrics_df.withColumn(
    "warehouse_loaded_at",
    current_timestamp()
)

execute_postgres_sql(
    "ALTER TABLE IF EXISTS merchant_metrics "
    "ADD COLUMN IF NOT EXISTS warehouse_loaded_at TIMESTAMP"
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
