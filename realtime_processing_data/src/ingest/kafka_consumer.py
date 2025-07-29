from kafka import KafkaConsumer
import json
import psycopg2
from config.settings import KAFKA_CONFIG, POSTGRES_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)

def connect_postgres():
    return psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        dbname=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"]
    )

def insert_article(cursor, data):
    sql = """
    INSERT INTO articles (title, link, description)
    VALUES (%s, %s, %s)
    """
    cursor.execute(sql, (
        data.get("title"),
        data.get("link"),
        data.get("description")
    ))

def consume_and_save():
    logger.info("🚀 Kafka Consumer đang hoạt động...")

    consumer = KafkaConsumer(
        KAFKA_CONFIG["topic"],
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        group_id=KAFKA_CONFIG["group_id"],
        auto_offset_reset=KAFKA_CONFIG["auto_offset_reset"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True
    )

    conn = connect_postgres()
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        for message in consumer:
            data = message.value
            logger.info(f"Nhận được từ Kafka: {data}")
            try:
                insert_article(cursor, data)
                logger.info("Ghi vào PostgreSQL thành công.")
            except Exception as e:
                logger.error(f"Lỗi ghi vào DB: {e}")
    except Exception as e:
        logger.error(f"Lỗi consumer: {e}")
    finally:
        cursor.close()
        conn.close()
