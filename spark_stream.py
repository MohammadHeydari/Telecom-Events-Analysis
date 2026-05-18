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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telecom-events") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

json_df = json_df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

agg = json_df.groupBy(
    window(col("timestamp"), "10 seconds"),
    col("operator")
).agg(
    sum("bytes").alias("total_bytes"),
    count("*").alias("events")
)

final_df = agg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("operator"),
    col("total_bytes"),
    col("events")
)

def write_to_clickhouse(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "telecom_agg") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

query = final_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("update") \
    .start()

query.awaitTermination()