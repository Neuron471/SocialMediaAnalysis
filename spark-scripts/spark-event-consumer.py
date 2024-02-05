import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from the .env file
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Retrieve Spark configuration from environment variables
spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# PostgreSQL database connection details
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Spark master URL
spark_host = f"spark://{spark_hostname}:{spark_port}"

# Set Spark submit arguments including required packages
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
 

# Create or retrieve SparkSession with appropriate configuration
spark = pyspark.sql.SparkSession.builder \
    .appName("SocialMedia") \
    .config("spark.master", spark_host) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

# Set log level for Spark
spark.sparkContext.setLogLevel("WARN")


# Define the schema for Kafka messages
kafka_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Kota", StringType(), True),
    StructField("Sosial_Media", StringType(), True),
    StructField("Usage_Count", IntegerType(), True),
    StructField("Sales", IntegerType(), True),
    StructField("Visitors", IntegerType(), True),
    StructField("PageVisitors", IntegerType(), True),
    StructField("Conversion", IntegerType(), True),
    StructField("Ts", IntegerType(), True),
])

# Read streaming data from Kafka
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Parse Kafka messages and apply transformations
parsed_df = stream_df.select(from_json(col("value").cast("string"), kafka_schema).alias("parsed_value")) \
    .select("parsed_value.*") \
    .withColumn("Ts", from_unixtime("Ts").cast(TimestampType())) \
    .withColumn("ConversionRate", expr("(Conversion/Visitors)*100")) \
    .withColumn("BounceRate", expr("(PageVisitors/Visitors)*100")) \
    .withWatermark("Ts", "10 minutes")  # Set watermark with a 5-minute handled late data

# Define the function to write to PostgreSQL
def write_to_postgresql(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    batch_df.show(truncate=False)

    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{PG_HOST}/{PG_DATABASE}") \
        .option("dbtable", "Socmed_user_sales") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .mode("append") \
        .save()

# Start the streaming query
query = (
    parsed_df.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgresql)
    .trigger(processingTime="10 seconds")  # Set processing time to 10 seconds
    .start()
)

# Await termination
query.awaitTermination()
