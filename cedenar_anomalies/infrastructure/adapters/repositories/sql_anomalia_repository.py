# cedenar_anomalies/infrastructure/adapters/repositories/sql_anomalia_repository.py
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import select

from cedenar_anomalies.domain.ports.repositories import IAnomaliaRepository
from cedenar_anomalies.infrastructure.database.models import AnomaliaData
from cedenar_anomalies.infrastructure.database.session import Session


class SQLAnomaliaRepository(IAnomaliaRepository):
    """Adaptador de repositorio SQL para anomalías (síncrono)"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def save(self, anomalia: AnomaliaData) -> AnomaliaData:
        with Session() as session:
            session.add(anomalia)
            session.commit()
            session.refresh(anomalia)
            return anomalia

    def save_batch(self, anomalias: List[AnomaliaData]) -> int:
        with Session() as session:
            session.add_all(anomalias)
            session.commit()
            return len(anomalias)

    def find_by_id(self, id: int) -> Optional[AnomaliaData]:
        with Session() as session:
            return session.get(AnomaliaData, id)

    def find_all(self) -> List[AnomaliaData]:
        with Session() as session:
            result = session.execute(select(AnomaliaData)).scalars().all()
            return list(result)

    def find_by_criteria(self, criteria: Dict[str, Any]) -> List[AnomaliaData]:
        query = select(AnomaliaData)

        with Session() as session:
            for field, value in criteria.items():
                if hasattr(AnomaliaData, field):
                    query = query.where(getattr(AnomaliaData, field) == value)

            result = session.execute(query).scalars().all()
            return list(result)

    def save_from_dataframe(self, df: pd.DataFrame) -> int:
        """Guarda anomalías desde un DataFrame"""
        self.logger.info(
            f"Procesando DataFrame con {len(df)} filas para guardar en la base de datos"
        )

        # Asegurarse de que exista la columna fecha_creacion
        if "fecha_creacion" not in df.columns:
            df["fecha_creacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Convertir DataFrame a objetos del dominio
        anomalias = []
        for _, row in df.iterrows():
            # Filtrar campos válidos para el modelo
            valid_fields = {k: v for k, v in row.items() if hasattr(AnomaliaData, k)}
            anomalias.append(AnomaliaData(**valid_fields))

        # Guardar en lotes
        batch_size = 1000
        saved_count = 0

        for i in range(0, len(anomalias), batch_size):
            batch = anomalias[i : i + batch_size]
            saved_count += self.save_batch(batch)
            self.logger.info(
                f"Guardados {saved_count} de {len(anomalias)} registros ({saved_count / len(anomalias) * 100:.2f}%)"
            )

        return saved_count

    def find_by_year(self, year: int) -> List[AnomaliaData]:
        """Busca anomalías por año"""
        return self.find_by_criteria({"ano": year})
