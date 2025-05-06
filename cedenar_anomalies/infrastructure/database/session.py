# cedenar_anomalies/infrastructure/database/session.py
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from cedenar_anomalies.utils.config import SYNC_DATABASE_URL

# Crear Base para los modelos
Base = declarative_base()

# Crear motor de base de datos
engine = create_engine(SYNC_DATABASE_URL)

# Crear fábrica de sesiones
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_session() -> Session:
    """Proporciona una sesión de base de datos en un contexto"""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# Para compatibilidad con código existente
class SessionWrapper:
    def __enter__(self):
        self.session = SessionLocal()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()


# Mantener una función global Session() para compatibilidad
Session = SessionWrapper
