# cedenar_anomalies/application/predict_clusters.py

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
    Función principal para predecir clusters por zona usando modelos previamente entrenados.
    """
    logger.info("Iniciando predicción de clusters por zona...")

    # --- Configuración ---
    data_input_filename = "dataset_train_clean.csv"
    output_filename = "dataset_con_clusters.csv"
    data_path = data_processed_dir(data_input_filename)
    output_path = data_processed_dir(output_filename)

    if not Path(data_path).exists():
        logger.error(f"Archivo de predicción no encontrado: {data_path}")
        return

    # --- Cargar datos ---
    try:
        df = pd.read_csv(data_path)
        logger.info(f"Datos de entrada cargados correctamente. Shape: {df.shape}")
    except Exception as e:
        logger.exception(f"Error al cargar archivo de predicción: {e}")
        return

    # --- Cargar modelos y realizar predicciones ---
    try:
        pipe = PipelineClusterFzz(logger=logger)
        pipelines = pipe.load_pipelines()

        if not pipelines:
            logger.error("No se encontraron modelos entrenados para predecir.")
            return

        df_predicted = pipe.predict_all_zones(df, pipelines)

        if df_predicted.empty:
            logger.error("La predicción no generó resultados.")
            return

        df_predicted.to_csv(output_path, index=False)
        logger.info(f"Predicción completada y guardada en: {output_path}")

    except Exception as e:
        logger.exception(f"Error durante el proceso de predicción: {e}")


if __name__ == "__main__":
    main()
