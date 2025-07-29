from config.settings import POSTGRES_CONFIG, KAFKA_CONFIG, SPARK_CONFIG
from utils.logger import get_logger
from utils.scraper import scrape_website
from ingest.kafka_producer import produce_articles
from ingest.kafka_consumer import consume_and_save
import threading
import time

logger = get_logger(__name__)

def main():
    logger.info("🚀 Khởi động pipeline...")

    logger.debug(f"📦 PostgreSQL config: {POSTGRES_CONFIG}")
    logger.debug(f"📡 Kafka config: {KAFKA_CONFIG}")
    logger.debug(f"⚙️ Spark config: {SPARK_CONFIG}")

    consumer_thread = threading.Thread(target=consume_and_save, daemon=True)
    consumer_thread.start()
    logger.info("📡 Kafka consumer đang chạy ở background...")

    url = "https://thanhnien.vn/"
    logger.info(f"🔗 Đang cào dữ liệu từ: {url}")
    df = scrape_website(url)

    if df is not None and not df.empty:
        logger.info(f"✅ Thu được {len(df)} dòng dữ liệu.")
        print(df.to_string(index=False))

        produce_articles(df.to_dict(orient="records"))
        logger.info("Đã gửi dữ liệu vào Kafka.")
    else:
        logger.warning("Không thu được dữ liệu nào.")


if __name__ == "__main__":
    main()
