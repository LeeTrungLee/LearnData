import os
from dotenv import load_dotenv
import yaml

# Load .env
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, '..','.env')
load_dotenv(dotenv_path=ENV_PATH)


# Load config.yaml
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')
with open(CONFIG_PATH, 'r') as f:
    config_yaml = yaml.safe_load(f)

# PostgreSQL config
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "echo": config_yaml["postgres"].get("echo", False),
}

# Kafka config
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "topic": os.getenv("KAFKA_TOPIC"),
    "group_id": config_yaml["kafka"].get("group_id"),
    "auto_offset_reset": config_yaml["kafka"].get("auto_offset_reset"),
}

# Spark config
SPARK_CONFIG = {
    "app_name": os.getenv("SPARK_APP_NAME", "RealTimeProcessor"),
    "master": os.getenv("SPARK_MASTER", "spark://spark:7077"),
    "batch_duration": config_yaml["spark"].get("batch_duration", 5),
}
