# cedenar_anomalies/domain/services/anomalia_service.py
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from cedenar_anomalies.domain.ports.repositories import IAnomaliaRepository, IApiClient
from cedenar_anomalies.infrastructure.database.models import AnomaliaData


class AnomaliaService:
    """Servicio de dominio para operaciones con anomalías"""

    def __init__(
        self,
        repository: IAnomaliaRepository,
        api_client: Optional[IApiClient] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.repository = repository
        self.api_client = api_client
        self.logger = logger or logging.getLogger(__name__)

    def load_anomalias_from_csv(self, csv_path: str, year: int) -> int:
        """
        Carga anomalías desde un archivo CSV a la base de datos

        Args:
            csv_path: Ruta al archivo CSV
            year: Año de referencia para los datos

        Returns:
            int: Número de registros guardados
        """
        dtype_map = {
        "AREA": str,
        "item_288": "Int64",
        "odt": "Int64",
        "orden": "Int64",
        "PLAN_COMERCIAL": str,
        "Descripcion": str,
        "reincidente": str,
        "Anomalia_conf": str,
        "ZONA": str,
        "año": "Int64",
        "LATI_USU": float,
        "LONG_USU": float,
        "NIVEL": float,
        "item_68": bool,
        "item_74": bool,
        "item_237": bool,
        "item_248": str,
        "item_597": str,
        "item_602": str,
        "item_108": str,
        "item_43": str,
        "item_603": str,
        "item_599": str,
        "item_35": str,
        "item_598": str,
        "item_33": str,
        "item_601": str,
        "item_24": str,
        "item_23": str,
        }
        self.logger.info(f"Cargando datos desde {csv_path} para el año {year}")

        # Leer CSV
        df = pd.read_csv(csv_path, dtype=dtype_map, low_memory=False)

        # Asegurarse de que exista la columna 'ano'
        if "año" not in df.columns:
            self.logger.info(f"Añadiendo columna 'año' con valor {year}")
            df["año"] = year

        # Agregar columna id y fecha creacion
        df["id"] = [uuid.uuid4().hex for _ in range(len(df))]
        df["fecha_creacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Guardar datos
        saved_count = self.repository.save_from_dataframe(df)

        self.logger.info(f"Se guardaron {saved_count} registros en la base de datos")
        return saved_count

    async def load_anomalias_from_api(self) -> int:
        """
        Carga anomalías desde una API externa

        Returns:
            int: Número de registros guardados
        """
        if not self.api_client:
            self.logger.error("No se ha configurado un cliente de API")
            return 0

        self.logger.info("Obteniendo datos desde API externa")

        # Obtener datos de la API
        api_data = await self.api_client.fetch_anomalias()

        # Procesar datos
        anomalias = await self.api_client.process_api_data(api_data)

        if not anomalias:
            self.logger.warning("No se obtuvieron datos válidos desde la API")
            return 0

        # Guardar datos
        saved_count = self.repository.save_batch(anomalias)

        self.logger.info(f"Se guardaron {saved_count} registros desde la API")
        return saved_count

    def find_anomalias_by_year(self, year: int) -> List[AnomaliaData]:
        """
        Busca anomalías por año

        Args:
            year: Año a buscar

        Returns:
            List[AnomaliaData]: Lista de anomalías encontradas
        """
        return self.repository.find_by_year(year)

    def find_anomalias_by_criteria(self, criteria: Dict[str, Any]) -> List[AnomaliaData]:
        """
        Busca anomalías por criterios específicos

        Args:
            criteria: Diccionario con criterios de búsqueda

        Returns:
            List[AnomaliaData]: Lista de anomalías encontradas
        """
        return self.repository.find_by_criteria(criteria)
