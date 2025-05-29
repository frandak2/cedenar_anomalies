# cedenar_anomalies/infrastructure/adapters/repositories/sql_inference_repository.py
import logging

import pandas as pd

from cedenar_anomalies.infrastructure.database.models import InferenceModel
from cedenar_anomalies.infrastructure.database.session import Session


class SQLInferenceRepository:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def save_from_dataframe(self, df: pd.DataFrame, fecha_carga=None) -> int:
        """
        Guarda los datos del DataFrame en la base de datos.

        Args:
            df: DataFrame con los datos a guardar
            fecha_carga: Fecha de carga de los datos (opcional)

        Returns:
            Número de registros guardados
        """
        count = 0
        # Usar Session como context manager, igual que en SQLAnomaliaRepository
        with Session() as session:
            # Convertir columnas de DataFrame a formato compatible con la tabla
            # Renombrar columnas con nombres problemáticos
            df_copy = df.copy()
            if "kWh Rec" in df_copy.columns:
                df_copy = df_copy.rename(columns={"kWh Rec": "kWh_Rec"})
            if "Factor.1" in df_copy.columns:
                df_copy = df_copy.rename(columns={"Factor.1": "Factor_1"})
            if "id" in df_copy.columns:
                df_copy = df_copy.rename(columns={"id": "data_id"})

            # Transformar DataFrames a diccionarios para crear los objetos del modelo
            records = df_copy.to_dict(orient="records")

            # Crear y guardar los objetos en la base de datos
            for record in records:
                # Limpiamos cualquier valor NaN o None
                clean_record = {k: v for k, v in record.items() if pd.notna(v)}

                # Añadir fecha de carga si se proporciona
                if fecha_carga:
                    clean_record["fecha_carga"] = fecha_carga

                # Crear el objeto del modelo
                inference_model = InferenceModel(**clean_record)

                # Añadir a la sesión
                session.add(inference_model)
                count += 1

                # Cada 1000 registros, hacemos commit para liberar memoria
                if count % 1000 == 0:
                    session.commit()
                    self.logger.info(f"Guardados {count} registros")

            # Commit final
            session.commit()

        return count

    def get_all(self, limit=1000) -> list:
        """
        Obtiene todos los registros de inference_model

        Args:
            limit: Número máximo de registros a obtener

        Returns:
            Lista de objetos InferenceModel
        """
        with Session() as session:
            return session.query(InferenceModel).limit(limit).all()
