# cedenar_anomalies/application/load_to_anomalia_db.py
import argparse
import logging
from pathlib import Path

from cedenar_anomalies.domain.services.anomalia_service import AnomaliaService
from cedenar_anomalies.infrastructure.adapters.repositories.sql_anomalia_repository import (
    SQLAnomaliaRepository,
)
from cedenar_anomalies.utils.paths import data_processed_dir

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Carga datos de anomalías a la base de datos PostgreSQL"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Ruta al archivo CSV",
        default=data_processed_dir("dataset_limpio_2022.csv"),
    )
    parser.add_argument("--year", type=int, help="Año de los datos", default=2022)

    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"El archivo {file_path} no existe")
        return

    # Inicializar componentes
    repository = SQLAnomaliaRepository(logger=logger)
    service = AnomaliaService(repository, logger=logger)

    # Ejecutar caso de uso
    saved_count = service.load_anomalias_from_csv(str(file_path), args.year)

    logger.info(f"Proceso completado: se guardaron {saved_count} registros")


if __name__ == "__main__":
    main()
