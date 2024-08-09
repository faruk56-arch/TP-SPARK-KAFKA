from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration de la session Spark pour le streaming
spark = SparkSession.builder \
    .appName("SismicStreamingAnalysis") \
    .getOrCreate()
    

# Schema des donnees sismiques
sismic_schema = StructType([
    StructField("event_id", StringType()),
    StructField("date", TimestampType()),
    StructField("time", StringType()),
    StructField("secousse", DoubleType()),
    StructField("magnitude", DoubleType()),
    StructField("tension entre plaque", DoubleType())
])

# Lecture du flux Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sismic_topic") \
    .load()

# Conversion des messages Kafka en DataFrame
sismic_stream_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_stream_df = sismic_stream_df.select(from_json(col("value"), sismic_schema).alias("data")).select("data.*")

# Afficher les donnees du stream pour verification
query = parsed_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
