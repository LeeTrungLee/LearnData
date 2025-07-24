from kafka import KafkaProducer
import json
from config.settings import KAFKA_CONFIG

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

def send_to_kafka(df):
    producer = get_kafka_producer()
    topic = KAFKA_CONFIG['topic']
    
    for _, row in df.iterrows():
        data = row.to_dict()
        producer.send(topic, value=data)
        print(f"📤 Đã gửi: {data}")
    
    producer.flush()
