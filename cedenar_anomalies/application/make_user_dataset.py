# cedenar_anomalies/application/make_user_dataset.py
"""Agrega el dataset de anomalías (por fila) a nivel USUARIO para riesgo.

Target = severidad máxima (max puntaje) por usuario. Las features de perfil son
constantes por usuario (verificado), así que se toma la primera ocurrencia.
"""
import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.utils.paths import data_interim_dir

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PROFILE_COLS = [
    "LATI_USU",
    "LONG_USU",
    "LATI_TRAFO",
    "LONG_TRAFO",
    "TRAFO_OPEN",
    "FASES",
    "KVA",
    "AREA",
    "PLAN_COMERCIAL",
    "ZONA",
    "CATEGORIA",
    "SUB_CATEGORIA",
]


def main():
    data_path = data_interim_dir("01_dataset_train_clean.csv")
    if not Path(data_path).exists():
        logger.error("Archivo de entrada no encontrado: %s", data_path)
        return
    df = pd.read_csv(data_path)
    df = df.dropna(subset=["puntaje"]).copy()
    df["puntaje"] = df["puntaje"].astype(int)
    logger.info(
        "Filas por anomalía: %d | usuarios: %d", len(df), df["Usuario"].nunique()
    )

    agg = {c: "first" for c in PROFILE_COLS}
    agg["puntaje"] = "max"
    user_df = df.groupby("Usuario", as_index=False).agg(agg)
    logger.info("Dataset usuario: %s", user_df.shape)
    logger.info(
        "Distribución target (max puntaje):\n%s",
        user_df["puntaje"].value_counts().sort_index().to_string(),
    )

    out = data_interim_dir("02_dataset_train_user.csv")
    user_df.to_csv(out, index=False)
    logger.info("Guardado en: %s", out)


if __name__ == "__main__":
    main()
