# cedenar_anomalies/infrastructure/adapters/api/anomalia_api_client.py
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

from cedenar_anomalies.domain.ports.repositories import IApiClient
from cedenar_anomalies.infrastructure.database.models import AnomaliaData


class AnomaliaApiClient(IApiClient):
    """Adaptador para cliente de API de anomalías"""

    def __init__(self, api_url: str, logger: Optional[logging.Logger] = None):
        self.api_url = api_url
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_anomalias(self) -> Dict[str, Any]:
        """Obtiene datos de anomalías desde la API externa"""
        self.logger.info(f"Obteniendo datos desde API: {self.api_url}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.api_url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    self.logger.info("Datos obtenidos exitosamente desde la API")
                    return data
        except aiohttp.ClientError as e:
            self.logger.error(f"Error al obtener datos de la API: {e}")
            return {"anomalias": []}

    async def process_api_data(self, api_data: Dict[str, Any]) -> List[AnomaliaData]:
        """Procesa datos de la API y los convierte en entidades del dominio"""
        self.logger.info("Procesando datos de la API")

        if not api_data or "anomalias" not in api_data:
            self.logger.error(
                "Formato de datos incorrecto: no se encontró la clave 'anomalias'"
            )
            return []

        anomalias_raw = api_data["anomalias"]
        self.logger.info(f"Procesando {len(anomalias_raw)} registros desde la API")

        anomalias = []
        for anomalia_data in anomalias_raw:
            # Validar campos requeridos
            required_fields = [
                "odt",
                "orden",
                "LATI_USU",
                "LONG_USU",
                "PLAN_COMERCIAL",
                "ZONA",
            ]
            if not all(field in anomalia_data for field in required_fields):
                self.logger.warning(f"Registro incompleto, omitiendo: {anomalia_data}")
                continue

            # Asegurar presencia de campos requeridos
            try:
                # Valores por defecto
                default_values = {
                    "ano": datetime.now().year,
                    "fecha_creacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Anomalia_conf": "Anomalia",
                    "reincidente": "NO",
                    "item_68": False,
                    "item_74": False,
                    "item_237": False,
                    "item_248": "NO INDICA",
                    "item_597": "NOAPL",
                    "item_602": "NOAPL",
                    "item_108": "NO INDICA",
                    "item_43": "NO INDICA",
                    "item_603": "NOAPL",
                    "item_599": "NOAPL",
                    "item_35": "NO INDICA",
                    "item_598": "NOAPL",
                    "item_33": "NO INDICA",
                    "item_601": "NOAPL",
                    "item_24": "NO",
                    "item_23": "NO",
                    "NIVEL": 0.0,
                }

                # Aplicar valores por defecto si no existen
                for field, default_value in default_values.items():
                    if field not in anomalia_data:
                        anomalia_data[field] = default_value

                # Convertir tipos de datos
                anomalia_data["LATI_USU"] = float(anomalia_data["LATI_USU"])
                anomalia_data["LONG_USU"] = float(anomalia_data["LONG_USU"])
                anomalia_data["NIVEL"] = float(anomalia_data["NIVEL"])
                anomalia_data["item_288"] = int(anomalia_data.get("item_288", 0))
                anomalia_data["odt"] = int(anomalia_data["odt"])
                anomalia_data["orden"] = int(anomalia_data["orden"])

                # Filtrar campos válidos para el modelo
                valid_data = {
                    k: v for k, v in anomalia_data.items() if hasattr(AnomaliaData, k)
                }

                # Crear entidad
                anomalia = AnomaliaData(**valid_data)
                anomalias.append(anomalia)
            except Exception as e:
                self.logger.error(f"Error al procesar registro: {e}, {anomalia_data}")

        self.logger.info(f"Se procesaron {len(anomalias)} registros válidos de la API")
        return anomalias


# Adaptador para pruebas con datos de prueba (Mock)
class MockAnomaliaApiClient(IApiClient):
    """Cliente de API mock para pruebas"""

    def __init__(self, mock_file: str, logger: Optional[logging.Logger] = None):
        self.mock_file = mock_file
        self.logger = logger or logging.getLogger(__name__)
        self.api_client = None  # Se inicializará bajo demanda

    async def fetch_anomalias(self) -> Dict[str, Any]:
        """Obtiene datos mock desde un archivo"""
        self.logger.info(f"Obteniendo datos mock desde: {self.mock_file}")

        try:
            with open(self.mock_file, "r") as f:
                data = json.load(f)
                self.logger.info("Datos mock cargados exitosamente")
                return data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error al cargar datos mock: {e}")
            return {"anomalias": []}

    async def process_api_data(self, api_data: Dict[str, Any]) -> List[AnomaliaData]:
        """Reutiliza la implementación real para procesar datos"""
        if not self.api_client:
            self.api_client = AnomaliaApiClient("http://mock", logger=self.logger)

        return await self.api_client.process_api_data(api_data)


# Función para crear datos mock
def create_mock_data(
    output_file: str, num_records: int = 10, logger: Optional[logging.Logger] = None
):
    """Crea un archivo JSON con datos de prueba"""
    logger = logger or logging.getLogger(__name__)
    logger.info(f"Generando {num_records} registros de prueba")

    import random

    # Datos base para generar valores aleatorios
    zonas = ["ZONA_NORTE", "ZONA_SUR", "ZONA_ESTE", "ZONA_OESTE", "ZONA_CENTRO"]
    planes = ["PLAN_A", "PLAN_B", "PLAN_C", "PLAN_D"]
    descripciones = [
        "Anomalía en consumo",
        "Conexión irregular",
        "Medidor manipulado",
        "Desconexión no autorizada",
        "Posible fraude",
    ]

    # Generar datos aleatorios
    anomalias = []
    for i in range(num_records):
        # Crear registro base
        registro = {
            "odt": random.randint(10000, 99999),
            "orden": random.randint(100000, 999999),
            "LATI_USU": round(random.uniform(1.0, 3.0), 6),
            "LONG_USU": round(random.uniform(-78.0, -76.0), 6),
            "NIVEL": round(random.uniform(1.0, 5.0), 1),
            "ZONA": random.choice(zonas),
            "PLAN_COMERCIAL": random.choice(planes),
            "Descripcion": random.choice(descripciones),
            "item_288": random.randint(1000, 9999),
            "item_68": random.choice([True, False]),
            "item_74": random.choice([True, False]),
            "item_237": random.choice([True, False]),
            "item_23": random.choice(["SI", "NO"]),
            "item_24": random.choice(["SI", "NO"]),
        }
        anomalias.append(registro)

    # Crear estructura completa
    mock_data = {"anomalias": anomalias}

    # Guardar a archivo
    with open(output_file, "w") as f:
        json.dump(mock_data, f, indent=2)

    logger.info(f"Datos mock guardados en {output_file}")
