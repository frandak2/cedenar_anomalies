import argparse
import logging
from pathlib import Path

from cedenar_anomalies.domain.services.inference_service import InferenceService
from cedenar_anomalies.infrastructure.adapters.repositories.sql_inference_repository import (
    SQLInferenceRepository,
)
from cedenar_anomalies.utils.paths import data_processed_dir

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Carga datos de inferencia a la base de datos PostgreSQL"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Ruta al archivo CSV",
        default=data_processed_dir("dataset_con_clusters.csv"),
        required=False,  # Cambiado a False porque ya tiene un default
    )

    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"El archivo {file_path} no existe")
        return

    # Inicializar componentes
    repository = SQLInferenceRepository(logger=logger)
    service = InferenceService(repository, logger=logger)

    # Ejecutar caso de uso
    saved_count = service.load_inference_from_csv(str(file_path))

    logger.info(f"Proceso completado: se guardaron {saved_count} registros")


if __name__ == "__main__":
    main()
