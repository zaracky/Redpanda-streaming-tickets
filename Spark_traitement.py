import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, when, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
RESULTS_FOLDER  = os.getenv("RESULTS_FOLDER")
CHECKPOINTS_FOLDER  = os.getenv("CHECKPOINTS_FOLDER")

# 🔧 Session Spark avec S3
spark = SparkSession.builder \
    .appName("KafkaRedpandaConsumerToS3") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .getOrCreate()

# Schéma
schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("request", StringType(), True),
    StructField("request_type", StringType(), True),
    StructField("priority", StringType(), True)
])

# Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
    .option("kafka.sasl.username", "superuser") \
    .option("kafka.sasl.password", "secretpassword") \
    .load()

# Parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Support team
df_transformed = df_parsed.withColumn(
    "support_team",
    when(col("request_type") == "Technical", "Tech Support")
    .when(col("request_type") == "Billing", "Billing Support")
    .otherwise("General Support")
)

# Cast timestamp string → timestamp
df_with_ts = df_transformed.withColumn("ts", col("timestamp").cast(TimestampType()))

# Watermark sur le timestamp (ici, 1 minute de retard)
df_with_watermark = df_with_ts.withWatermark("ts", "1 minute")

# Group by window + request_type
df_windowed = df_with_watermark.groupBy(
    window(col("ts"), "10 seconds"),
    col("request_type")
).agg(count("*").alias("ticket_count"))

# Write in Parquet to S3 (append mode)
query_parquet = df_windowed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", RESULTS_FOLDER) \
    .option("checkpointLocation", CHECKPOINTS_FOLDER) \
    .start()

# Also print to console
query_console = df_windowed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_parquet.awaitTermination()
query_console.awaitTermination()
