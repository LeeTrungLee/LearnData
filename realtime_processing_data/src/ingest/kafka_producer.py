from kafka import KafkaProducer
import json
from config.settings import KAFKA_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_articles(data_list):
    for data in data_list:
        try:
            producer.send(KAFKA_CONFIG["topic"], value=data)
            logger.info(f"📤 Gửi vào Kafka: {data}")
        except Exception as e:
            logger.error(f"❌ Lỗi gửi Kafka: {e}")
    producer.flush()
