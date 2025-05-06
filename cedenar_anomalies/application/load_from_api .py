# cedenar_anomalies/application/load_from_api.py
import argparse
import asyncio
import logging

from cedenar_anomalies.domain.services.anomalia_service import AnomaliaService
from cedenar_anomalies.infrastructure.adapters.api.anomalia_api_client import (
    AnomaliaApiClient,
    MockAnomaliaApiClient,
    create_mock_data,
)
from cedenar_anomalies.infrastructure.adapters.repositories.async_sql_anomalia_repository import (
    AsyncSQLAnomaliaRepository,
)

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_api_load(api_url=None, mock_file=None):
    """Ejecuta el caso de uso de carga desde API"""

    # Inicializar repositorio
    repository = AsyncSQLAnomaliaRepository(logger=logger)

    # Inicializar cliente de API
    if api_url:
        api_client = AnomaliaApiClient(api_url, logger=logger)
    elif mock_file:
        api_client = MockAnomaliaApiClient(mock_file, logger=logger)
    else:
        logger.error("Debe especificar una URL de API o un archivo mock")
        return

    # Inicializar servicio
    service = AnomaliaService(repository, api_client, logger=logger)

    # Ejecutar caso de uso
    await service.load_anomalias_from_api()

    logger.info("Proceso de carga desde API completado")


def main():
    parser = argparse.ArgumentParser(description="Procesa datos de anomalías desde API")
    parser.add_argument("--api", type=str, help="URL de la API externa")
    parser.add_argument(
        "--mock", type=str, help="Ruta al archivo JSON con datos de prueba"
    )
    parser.add_argument(
        "--create-mock",
        action="store_true",
        help="Crear archivo mock con datos de prueba",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="api_mock_data.json",
        help="Ruta para guardar el mock generado",
    )
    parser.add_argument(
        "--num-records",
        type=int,
        default=10,
        help="Número de registros a generar para el mock",
    )

    args = parser.parse_args()

    if args.create_mock:
        output_path = args.output
        create_mock_data(output_path, args.num_records, logger)
        return

    asyncio.run(run_api_load(args.api, args.mock))


if __name__ == "__main__":
    main()
