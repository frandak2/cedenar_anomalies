# cedenar_anomalies/application/train_clusters.py

import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelineClusterFzz,
)
from cedenar_anomalies.utils.paths import data_processed_dir

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Función principal para entrenar modelos de clustering por zona usando arquitectura hexagonal.
    """
    logger.info("Iniciando entrenamiento de modelos de clustering...")

    # Configuración
    data_filename = "dataset_train_clean.csv"
    data_path = data_processed_dir(data_filename)

    if not Path(data_path).exists():
        logger.error(f"Archivo de entrenamiento no encontrado: {data_path}")
        return

    # Cargar datos
    try:
        df = pd.read_csv(data_path)
        logger.info(f"Datos cargados correctamente. Shape: {df.shape}")
    except Exception as e:
        logger.exception(f"Error al cargar el archivo: {e}")
        return

    # Entrenar modelos por zona
    try:
        pipe = PipelineClusterFzz(logger=logger)
        pipelines = pipe.train_by_zone(df)

        if not pipelines:
            logger.error("No se entrenó ningún modelo. Verifica los datos de entrada.")
            return

        logger.info("Entrenamiento completado exitosamente para todas las zonas.")
    except Exception as e:
        logger.exception(f"Error durante el entrenamiento: {e}")


if __name__ == "__main__":
    main()
