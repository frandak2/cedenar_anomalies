import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.utils.paths import data_interim_dir, data_raw_dir

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

# Columnas del contrato del dashboard de Looker (dimensiones/filtros), NO son
# features del modelo; se arrastran tal cual desde el maestro de usuarios.
EXTRA_CONTRACT_COLS = ["BARRIO_PRODUCTO", "MUNICIPIO_PRODUCTO", "SECCIONAL"]


def main():
    """Construye el dataset de inferencia a nivel usuario desde el maestro.

    Una fila por usuario (PRODUCTO) con sus features de perfil. Se scorean todos
    los usuarios con zona y coordenadas; predict_all_zones luego conserva solo
    las zonas con modelo de cluster entrenado.
    """
    logger.info("Construyendo dataset de inferencia a nivel usuario...")
    user_file = data_raw_dir("cedenar_data.xlsx")
    if not Path(user_file).exists():
        logger.error("Maestro de usuarios no encontrado: %s", user_file)
        return

    df = pd.read_excel(user_file)
    logger.info("Maestro cargado: %s", df.shape)

    df = df.rename(columns={"PRODUCTO": "Usuario"})
    cols = ["Usuario"] + PROFILE_COLS + EXTRA_CONTRACT_COLS
    faltan = [c for c in cols if c not in df.columns]
    if faltan:
        logger.error("Faltan columnas en el maestro: %s", faltan)
        return
    df = df[cols].copy()

    # Se requiere zona y coordenadas para clusterizar
    df = df.dropna(subset=["ZONA", "LATI_USU", "LONG_USU"]).copy()
    # Una fila por usuario
    df = df.drop_duplicates(subset=["Usuario"]).copy()

    # kWh Rec a nivel usuario = suma histórica de la energía recuperada de sus
    # anomalías (igual que antes era por anomalía; al sumar por cluster da el
    # mismo total). NULL para usuarios sin anomalías registradas.
    anomalies_file = data_raw_dir("anomalias 2023-2026.xlsx")
    if Path(anomalies_file).exists():
        anom = pd.read_excel(anomalies_file, usecols=["Usuario", "kWh Rec"])
        kwh = anom.groupby("Usuario", as_index=False)["kWh Rec"].sum()
        df = df.merge(kwh, on="Usuario", how="left")
        logger.info(
            "kWh Rec agregado: %d usuarios con energía recuperada",
            int(df["kWh Rec"].notna().sum()),
        )
    else:
        logger.warning(
            "Archivo de anomalías no encontrado: %s; kWh Rec quedará vacío",
            anomalies_file,
        )
        df["kWh Rec"] = pd.NA

    logger.info(
        "Dataset de inferencia: %s | usuarios: %d | zonas: %s",
        df.shape,
        df["Usuario"].nunique(),
        sorted(df["ZONA"].unique().tolist()),
    )

    out = data_interim_dir("dataset_to_inference.csv")
    df.to_csv(out, index=False)
    logger.info("Guardado en: %s", out)


if __name__ == "__main__":
    main()
