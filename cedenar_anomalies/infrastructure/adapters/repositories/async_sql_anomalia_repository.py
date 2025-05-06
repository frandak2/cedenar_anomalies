# cedenar_anomalies/infrastructure/adapters/repositories/async_sql_anomalia_repository.py
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import select

from cedenar_anomalies.domain.ports.repositories import IAnomaliaRepository
from cedenar_anomalies.infrastructure.database.models import AnomaliaData
from cedenar_anomalies.infrastructure.database.session import async_session


class AsyncSQLAnomaliaRepository(IAnomaliaRepository):
    """Adaptador de repositorio SQL para anomalías (asíncrono)"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    async def _save_async(self, anomalia: AnomaliaData) -> AnomaliaData:
        async with async_session() as session:
            session.add(anomalia)
            await session.commit()
            await session.refresh(anomalia)
            return anomalia

    async def _save_batch_async(self, anomalias: List[AnomaliaData]) -> int:
        async with async_session() as session:
            session.add_all(anomalias)
            await session.commit()
            return len(anomalias)

    # Implementaciones sincrónicas que delegan a las asíncronas
    def save(self, anomalia: AnomaliaData) -> AnomaliaData:
        """Versión síncrona que ejecuta la asíncrona"""
        return asyncio.run(self._save_async(anomalia))

    def save_batch(self, anomalias: List[AnomaliaData]) -> int:
        """Versión síncrona que ejecuta la asíncrona"""
        return asyncio.run(self._save_batch_async(anomalias))

    async def _find_by_id_async(self, id: int) -> Optional[AnomaliaData]:
        async with async_session() as session:
            return await session.get(AnomaliaData, id)

    def find_by_id(self, id: int) -> Optional[AnomaliaData]:
        return asyncio.run(self._find_by_id_async(id))

    async def _find_all_async(self) -> List[AnomaliaData]:
        async with async_session() as session:
            result = await session.execute(select(AnomaliaData))
            return list(result.scalars().all())

    def find_all(self) -> List[AnomaliaData]:
        return asyncio.run(self._find_all_async())

    async def _find_by_criteria_async(
        self, criteria: Dict[str, Any]
    ) -> List[AnomaliaData]:
        query = select(AnomaliaData)

        async with async_session() as session:
            for field, value in criteria.items():
                if hasattr(AnomaliaData, field):
                    query = query.where(getattr(AnomaliaData, field) == value)

            result = await session.execute(query)
            return list(result.scalars().all())

    def find_by_criteria(self, criteria: Dict[str, Any]) -> List[AnomaliaData]:
        return asyncio.run(self._find_by_criteria_async(criteria))

    async def _save_dataframe_async(self, df: pd.DataFrame) -> int:
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
            saved_count += await self._save_batch_async(batch)
            self.logger.info(
                f"Guardados {saved_count} de {len(anomalias)} registros ({saved_count / len(anomalias) * 100:.2f}%)"
            )

        return saved_count

    def save_from_dataframe(self, df: pd.DataFrame) -> int:
        return asyncio.run(self._save_dataframe_async(df))

    def find_by_year(self, year: int) -> List[AnomaliaData]:
        return self.find_by_criteria({"ano": year})
