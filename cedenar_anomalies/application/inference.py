# cedenar_anomalies/application/inference.py

import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelineClusterFzz, PipelinePuntaje
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
    output_filename = "dataset_inference.csv"
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

    # --- Cargar modelos y realizar predicciones de clusters por zona ---
    try:
        pipe_cluster = PipelineClusterFzz(logger=logger)
        pipelines_cluster = pipe_cluster.load_pipelines()

        if not pipelines_cluster:
            logger.error("No se encontraron modelos entrenados para predecir.")
            return

        df_predicted_cluster = pipe_cluster.predict_all_zones(df, pipelines_cluster)

        if df_predicted_cluster.empty:
            logger.error("La predicción de cluster no generó resultados.")
            return

        pipe_puntaje = PipelinePuntaje(logger=logger)
        pipeline_puntaje = pipe_puntaje.load_pipeline()

        if not pipeline_puntaje:
            logger.error("No se encontraron modelos entrenados para predecir.")
            return

        df_predicted_puntaje = pipe_puntaje.predict(pipeline_puntaje, df_predicted_cluster)

        if df_predicted_puntaje.empty:
            logger.error("La predicción de puntaje no generó resultados.")
            return

        df_predicted_puntaje.to_csv(output_path, index=False)
        logger.info(f"Predicción completada y guardada en: {output_path}")

    except Exception as e:
        logger.exception(f"Error durante el proceso de predicción: {e}")


if __name__ == "__main__":
    main()
