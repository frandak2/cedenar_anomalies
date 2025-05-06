# cedenar_anomalies/main.py
import argparse
import asyncio
import logging
import sys
from pathlib import Path

from cedenar_anomalies.domain.services.anomalia_service import AnomaliaService
from cedenar_anomalies.infrastructure.adapters.api.anomalia_api_client import (
    AnomaliaApiClient,
    MockAnomaliaApiClient,
    create_mock_data,
)
from cedenar_anomalies.infrastructure.adapters.repositories.sql_anomalia_repository import (
    SQLAnomaliaRepository,
)
from cedenar_anomalies.infrastructure.database.init_db import init_db
from cedenar_anomalies.utils.async_alembic import (
    downgrade_one_revision,
    downgrade_to_revision,
    generate_migration,
    get_current_revision,
    upgrade_to_head,
    upgrade_to_revision,
)
from cedenar_anomalies.utils.config import PROCESSED_DATA_DIR

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def load_from_api(api_url=None, mock_file=None):
    """Ejecuta el caso de uso de carga desde API"""

    # Inicializar repositorio
    repository = SQLAnomaliaRepository(logger=logger)

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
    count = await service.load_anomalias_from_api()

    logger.info(f"Proceso de carga desde API completado: {count} registros guardados")


def load_from_csv(file_path, year):
    """Ejecuta el caso de uso de carga desde CSV"""

    if not file_path.exists():
        logger.error(f"El archivo {file_path} no existe")
        return

    # Inicializar componentes
    repository = SQLAnomaliaRepository(logger=logger)
    service = AnomaliaService(repository, logger=logger)

    # Ejecutar caso de uso
    saved_count = service.load_anomalias_from_csv(str(file_path), year)

    logger.info(f"Proceso completado: se guardaron {saved_count} registros")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cedenar Anomalías - Gestión de datos")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponibles")

    # init-db
    init_parser = subparsers.add_parser("init-db", help="Inicializar base de datos")
    init_parser.add_argument(
        "--drop", action="store_true", help="Eliminar tablas existentes"
    )

    # load-csv
    csv_parser = subparsers.add_parser("load-csv", help="Cargar datos desde CSV")
    csv_parser.add_argument(
        "--file", type=str, default=str(PROCESSED_DATA_DIR / "dataset_limpio_2023.csv")
    )
    csv_parser.add_argument("--year", type=int, default=2023)

    # load-api
    api_parser = subparsers.add_parser("load-api", help="Cargar datos desde API")
    api_parser.add_argument("--api", type=str)
    api_parser.add_argument("--mock", type=str)

    # create-mock
    mock_parser = subparsers.add_parser("create-mock", help="Crear archivo mock")
    mock_parser.add_argument("--output", type=str, default="api_mock_data.json")
    mock_parser.add_argument("--num-records", type=int, default=10)

    # migration
    migration_parser = subparsers.add_parser("migration", help="Migraciones")
    migration_parser.add_argument("--generate", action="store_true")
    migration_parser.add_argument("--message", type=str, default="Auto migration")
    migration_parser.add_argument("--upgrade", action="store_true")
    migration_parser.add_argument("--downgrade", action="store_true")
    migration_parser.add_argument("--to", type=str)
    migration_parser.add_argument("--current", action="store_true")

    return parser


async def handle_migration(args):
    if args.current:
        print(f"Revisión actual: {await get_current_revision()}")
    elif args.generate:
        await generate_migration(args.message)
    elif args.upgrade and args.to:
        await upgrade_to_revision(args.to)
    elif args.downgrade and args.to:
        await downgrade_to_revision(args.to)
    elif args.upgrade:
        await upgrade_to_head()
    elif args.downgrade:
        await downgrade_one_revision()
    else:
        print(
            "Debe especificar una acción: --generate, --upgrade, --downgrade o --current"
        )


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "init-db":
        init_db(drop_tables=args.drop)
    elif args.command == "load-csv":
        load_from_csv(Path(args.file), args.year)
    elif args.command == "load-api":
        asyncio.run(load_from_api(args.api, args.mock))
    elif args.command == "create-mock":
        create_mock_data(args.output, args.num_records, logger)
    elif args.command == "migration":
        asyncio.run(handle_migration(args))


if __name__ == "__main__":
    main()
