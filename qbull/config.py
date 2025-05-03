import os
from pathlib import Path
from dotenv import load_dotenv

env = os.getenv("ENV", "development")

env_file = {"development": ".env.development", "production": ".env.production"}.get(
    env, ".env.example"
)

env_path = Path(__file__).parent.parent / env_file
load_dotenv(dotenv_path=env_path)

PARTITIONS = int(os.getenv("PARTITIONS", 1))

PARTITIONS_PER_POD = int(os.getenv("PARTITIONS_PER_POD", 1))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
