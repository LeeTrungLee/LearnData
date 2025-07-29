from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType
import logging

# Cấu hình logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark_stream")

def start_streaming():
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema của dữ liệu Kafka gửi
    schema = StructType() \
        .add("title", StringType()) \
        .add("link", StringType()) \
        .add("description", StringType()) \
        .add("published_time", TimestampType())

    # Đọc dữ liệu từ Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sensor_data") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    def log_batch(df, epoch_id):
        count = df.count()
        logger.info(f"📦 Batch {epoch_id} nhận được {count} bản ghi:")
        df.show(truncate=False)

    query = df_parsed.writeStream \
        .foreachBatch(log_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
