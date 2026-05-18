<<<<<<< HEAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("TelecomStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("msisdn", StringType()),
    StructField("operator", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("cell_id", StringType()),
    StructField("bytes", IntegerType()),
    StructField("duration", IntegerType())
])

# Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telecom-events") \
    .option("startingOffsets", "latest") \
    .load()

# parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# aggregation
agg = json_df.groupBy(
    window(col("timestamp"), "10 seconds"),
    col("operator")
).agg(
    sum("bytes").alias("total_bytes"),
    count("*").alias("events")
)

query = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

=======
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("TelecomStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("msisdn", StringType()),
    StructField("operator", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("cell_id", StringType()),
    StructField("bytes", IntegerType()),
    StructField("duration", IntegerType())
])

# Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telecom-events") \
    .option("startingOffsets", "latest") \
    .load()

# parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# aggregation
agg = json_df.groupBy(
    window(col("timestamp"), "10 seconds"),
    col("operator")
).agg(
    sum("bytes").alias("total_bytes"),
    count("*").alias("events")
)

query = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

>>>>>>> aaa07d8 (added spark_stream.py)
query.awaitTermination()