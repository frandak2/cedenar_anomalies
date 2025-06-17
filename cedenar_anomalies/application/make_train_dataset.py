import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.data_processing_service import (
    DataProcessingService,
)

# Importar utilidades para gestión de rutas
from cedenar_anomalies.utils.paths import data_raw_dir, data_interim_dir

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Función principal para configurar y ejecutar el procesamiento de datos.
    """
    logger.info("Iniciando procesamiento de datos con arquitectura hexagonal...")

    try:
        # --- Variables de Configuración ---
        # Definir nombres de archivos de entrada (relativos a data_raw_dir)
        user_data_filename = "cedenar_data.xlsx"
        ponderado_filename = "items anomalia ponderado.xlsx"
        anomalies_filename = "anomalias 2022 23 y 24.xlsx"

        # Configuración para archivos de salida
        processed_output = "01_dataset_train_clean.csv"
        # --- Fin de Configuración ---

        logger.info("Configuración cargada con éxito.")
        logger.info(f"  Archivo de datos de usuario: {user_data_filename}")
        logger.info(f"  Archivo de conversión UID: {ponderado_filename}")
        logger.info(f"  Archivo de anomalías: {anomalies_filename}")

        # --- Paso 1: Cargar datos ---
        logger.info("Cargando datos de entrada...")

        # Crear rutas completas a los archivos
        anomalies_file = data_raw_dir(anomalies_filename)
        user_file = data_raw_dir(user_data_filename)
        ponderado_file = data_raw_dir(ponderado_filename)

        # Cargar datos usando pandas directamente
        anomalies_df = (
            pd.read_excel(anomalies_file) if Path(anomalies_file).exists() else None
        )

        try:
            users_df = pd.read_excel(user_file) if Path(user_file).exists() else None
        except Exception as e:
            logger.warning(f"Error al cargar archivo de usuarios: {e}")
            users_df = None

        try:
            ponderado_df = (
                pd.read_excel(ponderado_file) if Path(ponderado_file).exists() else None
            )
        except Exception as e:
            logger.warning(f"Error al cargar archivo de conversión UID: {e}")
            ponderado_df = None

        if anomalies_df is None:
            logger.error(f"No se pudo cargar el archivo de anomalías: {anomalies_file}")
            return

        logger.info("Datos cargados con éxito.")

        # --- Paso 2: Procesar datos ---
        logger.info("Iniciando procesamiento de datos...")

        # Crear servicio de procesamiento con solo los parámetros que acepta
        processing_service = DataProcessingService(logger=logger)

        # Procesar datos
        processed_df = processing_service.merge_and_clean(
            anomalies_df=anomalies_df,
            users_df=users_df,
            ponderado_df=ponderado_df,
        )

        if processed_df is None or processed_df.empty:
            logger.error("El procesamiento no retornó un DataFrame válido. Abortando.")
            return

        logger.info(
            f"Procesamiento inicial completado. DataFrame shape: {processed_df.shape}"
        )

        # Guardar datos procesados directamente con pandas
        processed_output_path = data_interim_dir(processed_output)
        processed_df.to_csv(processed_output_path, index=False)
        logger.info(f"Dataset procesado guardado en: {processed_output_path}")

        logger.info("Reporte de procesamiento generado")

    except FileNotFoundError as e:
        logger.error(
            f"Archivo no encontrado: {e}. Verifique los nombres de archivo y asegúrese "
            "de que existan en el directorio de datos sin procesar."
        )
    except KeyError as e:
        logger.error(
            f"Falta columna esperada durante el procesamiento: {e}.",
            " Compruebe la estructura del archivo de entrada.",
        )
    except Exception as e:
        logger.error(
            f"Ocurrió un error inesperado durante el procesamiento: {e}", exc_info=True
        )


if __name__ == "__main__":
    main()
