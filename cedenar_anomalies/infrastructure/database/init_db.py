# cedenar_anomalies/infrastructure/database/init_db.py
import logging

from sqlalchemy import create_engine

from cedenar_anomalies.infrastructure.database.models import Base
from cedenar_anomalies.utils.config import SYNC_DATABASE_URL

# Configurar logging
logger = logging.getLogger(__name__)


def init_db(drop_tables=False):
    """Inicializa la base de datos"""
    logger.info("Inicializando base de datos...")

    # Crear motor de base de datos
    engine = create_engine(SYNC_DATABASE_URL)

    # Eliminar tablas si se especifica
    if drop_tables:
        logger.warning("Eliminando tablas existentes...")
        Base.metadata.drop_all(engine)

    # Crear tablas
    logger.info("Creando tablas...")
    Base.metadata.create_all(engine)

    logger.info("Base de datos inicializada correctamente")
