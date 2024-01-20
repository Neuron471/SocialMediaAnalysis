import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

PG_HOST = 'ep-winter-band-34900791.ap-southeast-1.aws.neon.tech'
PG_DATABASE = 'SocmedDB'
PG_USER = 'farahduta7'
PG_PASSWORD = 'Mf9qmk0iKZhU'

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())


kafka_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Kota", StringType(), True),
    StructField("Sosial_Media", StringType(), True),
    StructField("Usage_Count", IntegerType(), True),
    StructField("Sales", IntegerType(), True),
    StructField("Ts", IntegerType(), True),
])


stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = stream_df.select(from_json(col("value").cast("string"), kafka_schema).alias("parsed_value")) \
    .select("parsed_value.*") \
    .withColumn("Ts", from_unixtime("Ts").cast(TimestampType()))


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


query = (
    parsed_df.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgresql)
    .trigger(processingTime="5 seconds")
    .start()
)


query.awaitTermination()
