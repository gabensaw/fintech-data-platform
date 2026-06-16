from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    BooleanType
)

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("fraud_flag", BooleanType(), True),
    StructField("event_timestamp", StringType(), True)
])