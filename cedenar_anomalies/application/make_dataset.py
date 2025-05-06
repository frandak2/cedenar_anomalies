import logging
from pathlib import Path

import pandas as pd

from cedenar_anomalies.domain.services.data_cleaning_service import DataCleaningService

# Importar servicios del dominio
from cedenar_anomalies.domain.services.data_processing_service import (
    DataProcessingService,
)

# Importar utilidades para gestión de rutas
from cedenar_anomalies.utils.paths import data_processed_dir, data_raw_dir

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
        plain_data_filename = "plain3.csv"
        user_data_filename = "cedenar_data.xlsx"
        uid_conv_filename = "conversion uid orden.xlsx"
        anomalies_filename = "anomalias 2022 23 y 24.xlsx"

        # Definir parámetros de procesamiento
        target_year = 2023

        # Definir lista de IDs de ítems
        item_ids = [
            1442,
            8,
            237,
            23,
            24,
            33,
            598,
            601,
            43,
            99,
            35,
            111,
            599,
            108,
            603,
            190,
            192,
            588,
            248,
            591,
            602,
            41,
            211,
            74,
            1889,
            597,
            600,
            37,
            1407,
            1410,
            68,
            1328,
            1334,
            594,
            67,
            1408,
            1352,
            1405,
            1283,
            1343,
            1346,
            1292,
            1298,
            69,
            1331,
            202,
            1349,
        ]

        # Definir columnas a mantener después de limpieza
        columns_to_keep = [
            "AREA",
            "item_288",
            "odt",
            "orden",
            "PLAN_COMERCIAL",
            "Descripcion",
            "reincidente",
            "Anomalia_conf",
            "ZONA",
            "año",
            "LATI_USU",
            "LONG_USU",
            "NIVEL",
            "item_68",
            "item_74",
            "item_237",
            "item_248",
            "item_597",
            "item_602",
            "item_108",
            "item_43",
            "item_603",
            "item_599",
            "item_35",
            "item_598",
            "item_33",
            "item_601",
            "item_24",
            "item_23",
        ]

        # Configuración para archivos de salida
        processed_output = f"dataset_decantado_{target_year}.csv"
        cleaned_output = f"dataset_limpio_{target_year}.csv"
        # --- Fin de Configuración ---

        logger.info("Configuración cargada con éxito.")
        logger.info(f"  Año objetivo: {target_year}")
        logger.info(f"  IDs de ítems: {item_ids}")
        logger.info(f"  Archivo de datos planos: {plain_data_filename}")
        logger.info(f"  Archivo de datos de usuario: {user_data_filename}")
        logger.info(f"  Archivo de conversión UID: {uid_conv_filename}")
        logger.info(f"  Archivo de anomalías: {anomalies_filename}")

        # --- Paso 1: Cargar datos ---
        logger.info("Cargando datos de entrada...")

        # Crear rutas completas a los archivos
        anomalies_file = data_raw_dir(anomalies_filename)
        plain_file = data_raw_dir(plain_data_filename)
        user_file = data_raw_dir(user_data_filename)
        uid_conv_file = data_raw_dir(uid_conv_filename)

        # Cargar datos usando pandas directamente
        anomalies_df = (
            pd.read_excel(anomalies_file) if Path(anomalies_file).exists() else None
        )
        plain_df = (
            pd.read_csv(plain_file, low_memory=False)
            if Path(plain_file).exists()
            else None
        )

        try:
            users_df = pd.read_excel(user_file) if Path(user_file).exists() else None
        except Exception as e:
            logger.warning(f"Error al cargar archivo de usuarios: {e}")
            users_df = None

        try:
            uid_conversion_df = (
                pd.read_excel(uid_conv_file) if Path(uid_conv_file).exists() else None
            )
        except Exception as e:
            logger.warning(f"Error al cargar archivo de conversión UID: {e}")
            uid_conversion_df = None

        if anomalies_df is None:
            logger.error(f"No se pudo cargar el archivo de anomalías: {anomalies_file}")
            return

        if plain_df is None:
            logger.warning(f"No se pudo cargar el archivo de datos planos: {plain_file}")

        logger.info("Datos cargados con éxito.")

        # --- Paso 2: Procesar datos ---
        logger.info("Iniciando procesamiento de datos...")

        # Crear servicio de procesamiento con solo los parámetros que acepta
        processing_service = DataProcessingService(target_year=target_year, logger=logger)

        # Asignar la lista de item_ids al servicio directamente (si existe el atributo)
        # NOTA: Si esto causa problemas, es necesario modificar DataProcessingService
        processing_service.item_ids = item_ids

        # Procesar datos
        processed_df = processing_service.process_data(
            anomalies_df=anomalies_df,
            users_df=users_df,
            plain_df=plain_df,
            uid_conversion_df=uid_conversion_df,
        )

        if processed_df is None or processed_df.empty:
            logger.error("El procesamiento no retornó un DataFrame válido. Abortando.")
            return

        logger.info(
            f"Procesamiento inicial completado. DataFrame shape: {processed_df.shape}"
        )

        # Guardar datos procesados directamente con pandas
        processed_output_path = data_processed_dir(processed_output)
        processed_df.to_csv(processed_output_path, index=False)
        logger.info(f"Dataset procesado guardado en: {processed_output_path}")

        logger.info("Reporte de procesamiento generado")

        # --- Paso 3: Limpiar datos ---
        logger.info("Iniciando fase de limpieza de datos...")

        # Crear servicio de limpieza
        cleaning_service = DataCleaningService(
            df=processed_df,
            output_filename=cleaned_output,
            columns_to_keep=columns_to_keep,
            logger=logger,
        )

        # Ejecutar limpieza
        cleaned_df = cleaning_service.clean()

        if cleaned_df is None or cleaned_df.empty:
            logger.error("La limpieza no retornó un DataFrame válido. Abortando.")
            return

        # Guardar datos limpios directamente con pandas
        cleaned_output_path = data_processed_dir(cleaned_output)
        cleaned_df.to_csv(cleaned_output_path, index=False)

        logger.info("Proceso completo finalizado con éxito.")
        logger.info(f"Dataset limpio guardado en: {cleaned_output_path}")
        logger.info(f"Shape final del dataset: {cleaned_df.shape}")

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
