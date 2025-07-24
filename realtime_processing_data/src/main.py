from config.settings import POSTGRES_CONFIG, KAFKA_CONFIG, SPARK_CONFIG
from utils.logger import get_logger
from utils.scraper import scrape_website
from load.db_writer import write_to_postgres
from ingest.kafka_producer import send_to_kafka

logger = get_logger(__name__)

def main():
    logger.info("🚀 Khởi động pipeline...")

    # Debug cấu hình
    logger.debug(f"📦 PostgreSQL config: {POSTGRES_CONFIG}")
    logger.debug(f"📡 Kafka config: {KAFKA_CONFIG}")
    logger.debug(f"⚙️ Spark config: {SPARK_CONFIG}")

    # Nhập URL
    source = input("🔗 Nhập URL để cào dữ liệu: ").strip()

    # Cào dữ liệu
    df = scrape_website(source)

    if df is not None and not df.empty:
        logger.info(f"✅ Thu được {len(df)} dòng dữ liệu từ website.")
        print(df.to_string(index=False))

    else:
        logger.warning("⚠️ Không thu được dữ liệu nào từ URL đã nhập.")
    
    send_to_kafka(df)

if __name__ == "__main__":
    main()
