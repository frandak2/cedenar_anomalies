# cedenar_anomalies/domain/services/inference_service.py
import logging
from datetime import datetime

import pandas as pd


class InferenceService:
    def __init__(self, repository, logger=None):
        self.repository = repository
        self.logger = logger or logging.getLogger(__name__)

    def load_inference_from_csv(self, file_path: str) -> int:
        """
        Carga datos desde un archivo CSV a la tabla inference_model

        Args:
            file_path: Ruta al archivo CSV

        Returns:
            Número de registros guardados
        """
        self.logger.info(f"Cargando datos desde {file_path}")

        try:
            # Leer el archivo CSV
            df = pd.read_csv(file_path, low_memory=False)
            self.logger.info(
                f"Archivo CSV cargado con éxito. {len(df)} filas encontradas."
            )

            # Fecha actual para el registro
            fecha_carga = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Guardar en la base de datos
            saved_count = self.repository.save_from_dataframe(df, fecha_carga)
            self.logger.info(f"Se guardaron {saved_count} registros en la base de datos.")

            return saved_count
        except Exception as e:
            self.logger.error(f"Error al cargar datos desde CSV: {str(e)}")
            raise
