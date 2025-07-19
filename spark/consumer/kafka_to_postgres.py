from confluent_kafka import Consumer
import psycopg2
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'news-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['web_data'])

conn = psycopg2.connect(
    dbname="test",
    user="root",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS news (
        id SERIAL PRIMARY KEY,
        title TEXT,
        link TEXT UNIQUE,
        description TEXT,
        timestamp DOUBLE PRECISION
    );
""")
conn.commit()

print("Đang nhận dữ liệu từ Kafka và lưu vào PostgreSQL...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            cur.execute("""
                INSERT INTO news (title, link, description, timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING;
            """, (data['title'], data['link'], data['description'], data['timestamp']))
            conn.commit()
            print(f"✅ Lưu thành công: {data['title']}")
        except Exception as e:
            print("DB insert error:", e)

except KeyboardInterrupt:
    print("Dừng chương trình.")
finally:
    consumer.close()
    cur.close()
    conn.close()
