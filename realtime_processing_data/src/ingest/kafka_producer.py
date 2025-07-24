from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='host.docker.internal:9092')

for i in range(5):
    message = f"Test message {i}"
    producer.send('test_topic', message.encode('utf-8'))
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
