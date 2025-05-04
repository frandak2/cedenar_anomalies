import logging

# Use relative import since data_processor is in the same package directory
from data_processor import DataProcessor
from data_cleaner import DataCleaner  # Importar la nueva clase

# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Import path functions
from cedenar_anomalies.utils.paths import (
    data_processed_dir,
    data_raw_dir,
)


def main():
    """
    Main function to configure and run the Zentry data processing.
    """
    # --- Configuration Variables ---
    # Define input filenames (relative to data_raw_dir)
    plain_data_filename = "plain3.csv"
    user_data_filename = "cedenar_data.xlsx"
    uid_conv_filename = "conversion uid orden.xlsx"
    anomalies_filename = "anomalias 2022 23 y 24.xlsx"

    # Define Processing Parameters
    target_year = 2023
    
    # Define the list of item IDs
    item_ids = [
        1442, 8, 237, 23, 24, 33, 598, 601, 43, 99, 35, 111, 599, 108, 603,
        190, 192, 588, 248, 591, 602, 41, 211, 74, 1889, 597, 600, 37, 1407, 
        1410, 68, 1328, 1334, 594, 67, 1408, 1352, 1405, 1283, 1343, 1346, 
        1292, 1298, 69, 1331, 202, 1349,
    ]
    
    # Define columnas a mantener después de limpieza
    columns_to_keep = [
        "item_68", "item_248", "item_74", "item_597", "item_602", "item_108",
        "item_43", "item_603", "item_599", "item_35", "item_598", "item_33",
        "item_601", "item_24", "item_23", "item_237", "LATI_USU", "LONG_USU",
        "NIVEL", "AREA", "item_288", "odt", "orden", "PLAN_COMERCIAL", "Descripcion",
        "reincidente", 'Anomalia_conf', 'ZONA'
    ]
    
    # Configuración para archivos de salida
    processed_output = f"dataset_decantado_{target_year}.csv"
    cleaned_output = f"dataset_limpio_{target_year}.csv"
    # --- End Configuration ---

    logging.info("Starting data processing with configured parameters...")
    logging.info(f"  Target Year: {target_year}")
    logging.info(f"  Item IDs: {item_ids}")
    logging.info(f"  Plain Data File: {plain_data_filename}")
    logging.info(f"  User Data File: {user_data_filename}")
    logging.info(f"  UID Conversion File: {uid_conv_filename}")
    logging.info(f"  Anomalies File: {anomalies_filename}")

    try:
        # --- Paso 1: Ejecutar el procesador de datos ---
        processor = DataProcessor(
            plain_filename=plain_data_filename,
            user_filename=user_data_filename,
            uid_conversion_filename=uid_conv_filename,
            anomalies_filename=anomalies_filename,
            target_year=target_year,
            item_ids=item_ids,
        )
        
        # Ejecutar el procesamiento inicial
        processed_df = processor.run()

        if processed_df is None:
            logging.error("La fase de procesamiento no retornó un DataFrame válido. Abortando.")
            return
        
        logging.info(f"Procesamiento inicial completado. DataFrame shape: {processed_df.shape}")
        
        # No es necesario guardar el dataset procesado manualmente ya que processor.run()
        # probablemente ya lo guarda, pero por si acaso lo mantenemos
        processed_df.to_csv(data_processed_dir(processed_output), index=False)
        logging.info(f"Dataset procesado guardado en: {data_processed_dir(processed_output)}")
        
        # --- Paso 2: Ejecutar la limpieza de datos ---
        logging.info("Iniciando fase de limpieza de datos...")
        
        cleaner = DataCleaner(
            df=processed_df,
            output_filename=cleaned_output,
            columns_to_keep=columns_to_keep
        )
        
        # Ejecutar la limpieza
        cleaned_df = cleaner.clean()
        
        # Guardar el dataset limpio - no es necesario pasar el directorio
        # ya que save_results() usará data_processed_dir internamente
        cleaned_output_path = cleaner.save_results()
        
        logging.info(f"Proceso completo finalizado con éxito.")
        logging.info(f"Dataset limpio guardado en: {cleaned_output_path}")
        logging.info(f"Shape final del dataset: {cleaned_df.shape}")

    except FileNotFoundError as e:
        logging.error(
            f"Input file not found: {e}. Please check filenames and ensure "
            "they exist in the raw data directory."
        )
    except KeyError as e:
        logging.error(
            f"Missing expected column during processing: {e}. Check input file structure."
        )
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during processing: {e}", exc_info=True
        )


if __name__ == "__main__":
    main()