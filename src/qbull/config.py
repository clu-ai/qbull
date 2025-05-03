import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Basic logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

env = os.getenv("ENV", "development")

env_file = {"development": ".env.development", "production": ".env.production"}.get(
    env, ".env.example"
)

env_path = Path(__file__).parent.parent / env_file
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.info(f"Loaded environment variables from: {env_path}")
else:
    logging.warning(f"Environment file not found: {env_path}, using system env vars or defaults.")


PARTITIONS = int(os.getenv("PARTITIONS", "8")) # Total partitions in the system
PARTITIONS_PER_POD = int(os.getenv("PARTITIONS_PER_POD", "2")) # Partitions this instance tries to acquire
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Configuration for Consumer
DEFAULT_JOB_LOCK_TTL_MS = int(os.getenv("DEFAULT_JOB_LOCK_TTL_MS", "60000")) # 60 seconds
DEFAULT_PARTITION_LOCK_TTL_SEC = int(os.getenv("DEFAULT_PARTITION_LOCK_TTL_SEC", "90")) # 90 seconds
PARTITION_REFRESH_INTERVAL_SEC = int(os.getenv("PARTITION_REFRESH_INTERVAL_SEC", "30")) # 30 seconds - MUST be less than TTL/3 ideally
DEFAULT_CONSUMER_GROUP_NAME = os.getenv("DEFAULT_CONSUMER_GROUP_NAME", "qbull_workers")
DEFAULT_READ_BLOCK_MS = int(os.getenv("DEFAULT_READ_BLOCK_MS", "5000")) # 5 seconds