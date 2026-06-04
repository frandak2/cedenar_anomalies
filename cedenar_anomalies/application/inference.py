# cedenar_anomalies/application/inference.py
import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelineClusterFzz,
    PipelinePuntaje,
)
from cedenar_anomalies.utils.paths import data_interim_dir, data_processed_dir

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Inferencia de riesgo a nivel usuario: perfil -> cluster -> puntaje."""
    logger.info("Iniciando inferencia de riesgo a nivel usuario...")

    data_path = data_interim_dir("dataset_to_inference.csv")
    if not Path(data_path).exists():
        logger.error("Dataset de inferencia no encontrado: %s", data_path)
        return

    df = pd.read_csv(data_path)
    logger.info("Datos cargados. Shape: %s", df.shape)

    # --- Clustering por zona ---
    pipe_cluster = PipelineClusterFzz(logger=logger)
    pipelines_cluster = pipe_cluster.load_pipelines()
    if not pipelines_cluster:
        logger.error("No se encontraron pipelines de cluster entrenados.")
        return

    df = pipe_cluster.predict_all_zones(df, pipelines_cluster)
    if df.empty:
        logger.error("La predicción de cluster no generó resultados.")
        return

    # --- Modelo de riesgo (puntaje) ---
    pipe_puntaje = PipelinePuntaje(use_cluster_features=True, logger=logger)
    pipeline_puntaje = pipe_puntaje.load_pipeline()
    if not pipeline_puntaje:
        logger.error("No se encontró el modelo de puntaje entrenado.")
        return

    df = pipe_puntaje.predict(pipeline_puntaje, df)
    # puntaje_pred (1-5) es el riesgo predicho; renombrar para el dashboard
    df = df.rename(columns={"puntaje_pred": "puntaje"})

    # Columnas del CONTRATO de BigQuery/Looker que provienen de una anomalía
    # concreta y no aplican a nivel usuario; se conservan para NO romper el
    # esquema ni los tableros (filtros/agregaciones existentes):
    #   Ejecucion = fecha de scoring del riesgo; kWh Rec / Nombre = N/A (NULL).
    df["Ejecucion"] = pd.Timestamp.now().date()
    df["kWh Rec"] = pd.NA
    df["Nombre"] = pd.NA

    # Orden de columnas = contrato existente de la tabla Datos_Inference.
    out_cols = [
        "Usuario",
        "Ejecucion",
        "AREA",
        "PLAN_COMERCIAL",
        "Nombre",
        "kWh Rec",
        "cluster_id",
        "puntaje",
        "puntaje_1",
        "puntaje_2",
        "puntaje_3",
        "puntaje_4",
        "puntaje_5",
        "LATI_USU",
        "LONG_USU",
        "ZONA",
        "BARRIO_PRODUCTO",
        "MUNICIPIO_PRODUCTO",
        "SECCIONAL",
    ]
    faltan = [c for c in out_cols if c not in df.columns]
    if faltan:
        logger.warning("Columnas de salida ausentes (se omiten): %s", faltan)
        out_cols = [c for c in out_cols if c in df.columns]
    result = df[out_cols].copy()

    interim_out = data_interim_dir("dataset_inference.csv")
    result.to_csv(interim_out, index=False)
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    processed_out = data_processed_dir(f"dataset_inference_{ts}.csv")
    result.to_csv(processed_out, index=False)

    logger.info("Inferencia completada: %d usuarios", len(result))
    logger.info(
        "Distribución de puntaje (riesgo) predicho:\n%s",
        result["puntaje"].value_counts().sort_index().to_string(),
    )
    logger.info("Guardado en: %s y %s", interim_out, processed_out)


if __name__ == "__main__":
    main()
