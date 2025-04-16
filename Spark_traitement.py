import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType


ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
RESULTS_FOLDER  = os.getenv("RESULTS_FOLDER")
CHECKPOINTS_FOLDER  = os.getenv("CHECKPOINTS_FOLDER")

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("TicketAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINTS_FOLDER) \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "100") \
        .getOrCreate()

def process_stream(spark):
    # Définition du schéma
    ticket_schema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("demande", StringType(), True),
        StructField("demande_type", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("create_time", StringType(), True)
    ])

    # Lecture du stream
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092") \
        .option("subscribe", "client_tickets") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()

    # Parsing et conversion du timestamp
    parsed_df = df.select(
        from_json(col("value").cast("string"), ticket_schema).alias("data")
    ).select("data.*")

    # Convertir create_time en timestamp AVANT d'appliquer le watermark
    parsed_df = parsed_df.withColumn(
        "create_time",
        to_timestamp(col("create_time"))
    )

    # Fonction d'assignation
    def assign_team(demande_type):
        if demande_type == "Technical":
            return "Tech Support"
        elif demande_type == "Billing":
            return "Billing Support"
        return "General Support"

    # Enregistrer la fonction UDF
    from pyspark.sql.functions import udf
    assign_team_udf = udf(assign_team, StringType())

    # Enrichissement et watermark
    enriched_df = parsed_df \
        .withColumn("support_team", assign_team_udf(col("demande_type"))) \
        .withWatermark("create_time", "1 minute")

   # Agrégation par demande_type et support_team
    main_aggregation = enriched_df \
        .groupBy("demande_type", "support_team", window(col("create_time"), "1 minute")) \
        .count()

    main_query = main_aggregation \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", RESULTS_FOLDER) \
        .option("checkpointLocation", CHECKPOINTS_FOLDER) \
        .option("compression", "snappy") \
        .trigger(processingTime='1 minute') \
        .start()

    # Agrégation par priorité
    priority_stats = enriched_df \
        .groupBy("priority", window(col("create_time"), "1 minute")) \
        .count()

    priority_query = priority_stats \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", os.path.join(RESULTS_FOLDER, "priorities")) \
        .option("checkpointLocation", os.path.join(CHECKPOINTS_FOLDER, "priorities")) \
        .option("compression", "snappy") \
        .trigger(processingTime='1 minute') \
        .start()

    return [main_query, priority_query]

def main():
    spark = None
    queries = []

    try:
        spark = create_spark_session()
        queries = process_stream(spark)

        for query in queries:
            query.awaitTermination()

    except Exception as e:
        print(f"Erreur critique: {e}")

    finally:
        for query in queries:
            query.stop()
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
