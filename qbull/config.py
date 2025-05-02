import os
from pathlib import Path
from dotenv import load_dotenv

# Determina el entorno activo
env = os.getenv("ENV", "development")

# Ruta del archivo .env correspondiente
env_file = {"development": ".env.development", "production": ".env.production"}.get(
    env, ".env.example"
)

# Cargar variables desde el archivo
env_path = Path(__file__).parent.parent / env_file
load_dotenv(dotenv_path=env_path)

# Variables disponibles
PARTITIONS = int(os.getenv("PARTITIONS", 1))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
