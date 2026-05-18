from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum
from pyspark.sql.types import StructType, StringType, IntegerType

# Spark Session
spark = SparkSession.builder \
    .appName("TelecomStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("data_mb", IntegerType()) \
    .add("call_duration", IntegerType()) \
    .add("operator", StringType())

# Read from Kafka
# -------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.224.128:9092") \
    .option("subscribe", "telecom_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Transform
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Aggregation
agg_df = df.groupBy(
    window(col("timestamp"), "1 minute"),
    col("operator")
).agg(
    _sum("data_mb").alias("total_data_mb"),
    _sum("call_duration").alias("total_call_duration")
)

# Output (Console)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()