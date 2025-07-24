import pandas as pd
from sqlalchemy import create_engine
from config.settings import POSTGRES_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)

def write_to_postgres(df: pd.DataFrame, table_name: str):
    if df.empty:
        logger.warning("⚠️ DataFrame rỗng, không có gì để lưu vào database.")
        return

    try:
        # Tạo chuỗi kết nối
        user = POSTGRES_CONFIG["user"]
        password = POSTGRES_CONFIG["password"]
        host = POSTGRES_CONFIG["host"]
        port = POSTGRES_CONFIG["port"]
        db = POSTGRES_CONFIG["database"]
        echo = POSTGRES_CONFIG.get("echo", False)

        connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(connection_url, echo=echo)

        # Ghi vào PostgreSQL
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logger.info(f"✅ Đã lưu {len(df)} dòng vào bảng '{table_name}' trong PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Lỗi khi ghi vào PostgreSQL: {e}")
