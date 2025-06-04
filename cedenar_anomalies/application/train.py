# cedenar_anomalies/application/train.py

import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelineClusterFzz,
    PipelinePuntaje,
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

    try:
        # Entrenar modelos cluster por zona
        pipe = PipelineClusterFzz(logger=logger)
        pipelines = pipe.train_by_zone(df)

        if not pipelines:
            logger.error("No se entrenó ningún modelo. Verifica los datos de entrada.")
            return

        # Entrenar modelos puntaje
        best_params = {
            "n_estimators": 138,
            "learning_rate": 0.002,
            "num_leaves": 184,
            "max_depth": 3,
            "min_child_samples": 75,
            "max_bin": 145,
            "reg_alpha": 0.002,
            "reg_lambda": 3.794,
            "min_gain_to_split": 13.483,
            "bagging_fraction": 0.984,
            "bagging_freq": 4,
            "feature_fraction": 1.0,
        }

        pipe_puntaje = PipelinePuntaje(params=best_params, logger=logger)
        pipeline_puntaje = pipe_puntaje.fit(df)

        if not pipeline_puntaje:
            logger.error("No se entrenó ningún modelo. Verifica los datos de entrada.")
            return

        logger.info("Entrenamiento completado exitosamente para todas las zonas.")
    except Exception as e:
        logger.exception(f"Error durante el entrenamiento: {e}")


if __name__ == "__main__":
    main()
