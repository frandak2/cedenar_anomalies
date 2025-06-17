# cedenar_anomalies/utils/config.py
import os
from pathlib import Path

from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorios de datos
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = BASE_DIR / "models"

# Asegurar que existan los directorios
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

# Configuración de base de datos
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/postgres"
)

# URL versión síncrona para SQLAlchemy
SYNC_DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

# Configuración para carga de datos
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# Configuración de logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# URL API externa
API_URL = os.getenv("API_URL", "http://localhost:8000/api/anomalias")
API_KEY = os.getenv("API_KEY", "")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
