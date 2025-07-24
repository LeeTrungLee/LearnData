from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def start_spark_stream():

    # Khởi tạo SparkSession với Kafka connector
    spark = SparkSession.builder \
        .appName("KafkaToPostgres") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()


    # Giảm bớt log rác
    spark.sparkContext.setLogLevel("WARN")

    # Đọc dữ liệu từ Kafka topic
    df_kafka_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "raw-data") \
        .option("startingOffsets", "latest") \
        .load()

    # Chuyển đổi dữ liệu từ binary sang chuỗi
    df_parsed = df_kafka_raw.selectExpr("CAST(value AS STRING) as message")

    # Ghi ra console
    query = df_parsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()
