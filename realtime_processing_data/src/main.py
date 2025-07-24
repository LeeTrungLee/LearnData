from config.settings import POSTGRES_CONFIG, KAFKA_CONFIG, SPARK_CONFIG
from utils.logger import get_logger
from utils.scraper import scrape_website
from ingest.kafka_producer import send_to_kafka
from transform.spark_streaming import start_spark_stream

logger = get_logger(__name__)

def main():
    logger.info("🚀 Khởi động pipeline...")

    # Debug cấu hình
    logger.debug(f"📦 PostgreSQL config: {POSTGRES_CONFIG}")
    logger.debug(f"📡 Kafka config: {KAFKA_CONFIG}")
    logger.debug(f"⚙️ Spark config: {SPARK_CONFIG}")

    # ✅ Cào dữ liệu
    url = "https://thanhnien.vn/"
    logger.info(f"🔗 Đang cào dữ liệu từ: {url}")
    df = scrape_website(url)

    if df is not None and not df.empty:
        logger.info(f"✅ Thu được {len(df)} dòng dữ liệu.")
        print(df.to_string(index=False))

        # ✅ Gửi vào Kafka
        send_to_kafka(df)
        logger.info("📤 Đã gửi dữ liệu vào Kafka.")

        # ✅ Bắt đầu Spark đọc Kafka
        logger.info("⚙️ Bắt đầu Spark Structured Streaming...")
        start_spark_stream()

    else:
        logger.warning("⚠️ Không thu được dữ liệu nào từ URL.")

if __name__ == "__main__":
    main()
