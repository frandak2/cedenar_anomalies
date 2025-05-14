import logging
from datetime import datetime
from typing import Dict, Optional

import pandas as pd


class DataProcessingService:
    """
    Servicio de dominio para procesar datos de anomalías.

    Este servicio se encarga de transformar y combinar datos de diferentes fuentes
    para preparar un conjunto de datos consolidado para análisis de anomalías.
    """

    # Constantes del servicio
    ZONA_ID = 1
    ODT_NUMBER_ID = 288

    def __init__(
        self, target_year: Optional[int] = None, logger: Optional[logging.Logger] = None
    ):
        """
        Inicializa el servicio de procesamiento de datos.

        Args:
            target_year: Año objetivo para filtrar los datos
            logger: Logger opcional para registrar las operaciones
        """
        # Configuración básica
        self.target_year = target_year
        self.logger = logger or logging.getLogger(__name__)

        # Atributos para almacenar DataFrames
        self.df_plain = None
        self.df_users = None
        self.df_uid_conversion = None
        self.df_anomalies = None
        self.processed_data = None
        self.df_ponderado = None

        # Atributos para reportes y configuración
        self.item_ids = []
        self.report = {}

    def _process_anomalies(self) -> pd.DataFrame:
        """Filters and cleans the anomalies data for the target year."""
        if self.df_anomalies is None:
            raise ValueError("Anomalies data not loaded.")

        # Verificar que se haya proporcionado un año objetivo
        if self.target_year is None:
            error_msg = (
                "No se especificó un año objetivo (target_year)."
                "Este parámetro es obligatorio."
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Verificar si existe la columna 'año' en el DataFrame de anomalías
        if "año" not in self.df_anomalies.columns:
            self.logger.warning(
                "La columna 'año' no existe en el DataFrame de anomalías. Intentando extraer año de 'Ejecucion'..."
            )

            # Si aún no tenemos la columna año, crear la columna con el valor de target_year
            self.logger.warning(
                f"Usando el año objetivo {self.target_year} para todos los registros."
            )
            self.df_anomalies["año"] = self.target_year

            # Asegurar que la columna 'año' sea numérica
            self.df_anomalies["año"] = pd.to_numeric(
                self.df_anomalies["año"], errors="coerce"
            )
            self.logger.info(f"add year {self.target_year} on anomalies data...")

        self.logger.info(f"Processing anomalies for the year {self.target_year}...")

        # Filter by target year
        anom_filtered_year = self.df_anomalies[
            self.df_anomalies["año"] == self.target_year
        ].copy()
        self.logger.info(
            f"Found {len(anom_filtered_year)} anomaly records for {self.target_year}."
        )

        if anom_filtered_year.empty:
            self.logger.warning(f"No anomaly data found for the year {self.target_year}.")
            # Return essential columns if empty to prevent merge errors later
            return pd.DataFrame(columns=["Orden", "Descripcion"])

        # Clean duplicate 'Orden' based on 'Revision'
        # Convert relevant columns to string for comparison, handling potential NaNs
        anom_filtered_year["Orden"] = anom_filtered_year["Orden"].astype(
            str
        )  # Keep Orden type consistent if needed later
        anom_filtered_year["Revision"] = anom_filtered_year["Revision"].astype(str)
        anom_filtered_year["Codigo"] = anom_filtered_year["Codigo"].astype(
            str
        )  # As per notebook

        # Identify duplicates and NaN revisions
        is_duplicated_orden = anom_filtered_year.duplicated(subset="Orden", keep=False)
        is_nan_revision = (
            anom_filtered_year["Revision"].str.lower() == "nan"
        )  # Ensure case-insensitivity

        # Keep rows that are NOT (duplicated AND have NaN revision)
        anom_cleaned = anom_filtered_year[~(is_duplicated_orden & is_nan_revision)]
        self.logger.info(f"Cleaned anomalies data size: {len(anom_cleaned)} rows.")

        # Select and return relevant columns
        return anom_cleaned[["Orden", "Descripcion"]]

    def _merge_items(self, base_df: pd.DataFrame) -> pd.DataFrame:
        """Merges data for specified item IDs onto the base DataFrame."""
        if self.df_plain is None:
            raise ValueError("Plain data not loaded.")

        df_merged = base_df.copy()
        logging.info(f"Merging item data for IDs: {self.item_ids}")

        for item_id in self.item_ids:
            logging.debug(f"Processing item_id: {item_id}")
            # Ensure we are checking against the correct column ('id') in df_plain
            df_item_all = self.df_plain[self.df_plain["id"] == item_id]

            if df_item_all.empty:
                logging.warning(
                    f"No data found for item_id {item_id} in the plain dataset. Skipping merge for this ID."
                )
                # Create an empty column with the expected name and appropriate type (e.g., object or float)
                # This prevents errors later if subsequent code expects the column.
                # We'll default to object type, adjust if a specific type is known.
                df_merged[f"item_{item_id}"] = pd.NA
                df_merged[f"item_{item_id}"] = df_merged[f"item_{item_id}"].astype(
                    "object"
                )  # Or choose a default type
                continue  # Skip to the next id

            # Select only necessary columns and make a copy
            df_item = df_item_all[["odt", "value"]].copy()

            # Drop duplicates based on 'odt' *before* merging
            df_item = df_item.drop_duplicates(subset="odt", keep="first")

            # Check if df_item is empty *after* filtering and deduplication
            if df_item.empty:
                logging.warning(
                    f"No unique 'odt' data found for item_id {item_id} after deduplication. Skipping merge."
                )
                df_merged[f"item_{item_id}"] = pd.NA
                df_merged[f"item_{item_id}"] = df_merged[f"item_{item_id}"].astype(
                    "object"
                )
                continue

            # Perform the merge
            df_merged = pd.merge(
                df_merged,
                df_item,
                on="odt",
                how="left",  # Left join to keep all rows from the base df
            )

            # Rename the merged 'value' column
            # Check if 'value' column exists before renaming (it should after a successful merge)
            if "value" in df_merged.columns:
                df_merged = df_merged.rename(columns={"value": f"item_{item_id}"})
                logging.debug(
                    f"Successfully merged and renamed data for item_id: {item_id}"
                )
            else:
                # This case might happen if the merge somehow didn't add the column,
                # or if multiple merges happen without renaming in between (less likely with this loop structure).
                logging.error(
                    f"Column 'value' not found after merging item_id {item_id}. Cannot rename."
                )
                # Handle error: Maybe add the column manually with NAs or raise an exception
                df_merged[f"item_{item_id}"] = pd.NA
                df_merged[f"item_{item_id}"] = df_merged[f"item_{item_id}"].astype(
                    "object"
                )

        return df_merged

    def _apply_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies specific data type conversions based on notebook's final step."""
        logging.info("Applying final data type conversions...")
        # Define the target data types (adapt based on final desired schema)
        # Note: This is based on the notebook's example. Adjust if needed.
        # It's generally safer to let pandas infer types unless specific types are crucial.
        dtype_mapping = {
            "item_288": "int64",
            "PLAN_COMERCIAL": "object",
            "NIVEL": "float64",
            "LATI_USU": "float64",
            "LONG_USU": "float64",
            "AREA": "object",
            "Descripcion": "object",
            "odt": "int64",
            "orden": "int64",
            # Dynamically add item columns - assume float64 for numeric, object otherwise?
            # Or use notebook's specific types if available and ids match
        }
        # Add expected types for item columns based on the notebook's example:
        notebook_item_dtypes = {
            1442: "float64",
            8: "object",
            237: "object",
            23: "object",
            24: "object",
            33: "float64",
            598: "object",
            601: "object",
            43: "float64",
            99: "float64",
            35: "object",
            111: "float64",
            599: "object",
            108: "float64",
            603: "object",
            190: "float64",
            192: "object",
            588: "float64",
            248: "object",
            591: "float64",
            602: "object",
            41: "float64",
            211: "float64",
            74: "object",
            1889: "float64",
            597: "object",
            600: "object",
            37: "float64",
            1407: "object",
            1410: "object",
            68: "object",
            1328: "float64",
            1334: "object",
            594: "object",
            67: "object",
            1408: "object",
            1352: "float64",
            1405: "object",
            1283: "float64",
            1343: "float64",
            1346: "float64",
            1292: "float64",
            1298: "float64",
            69: "object",
            1331: "object",
            202: "float64",
            1349: "float64",
        }

        for item_id in self.item_ids:
            col_name = f"item_{item_id}"
            # Only add dtype mapping if the column actually exists in the DataFrame
            if col_name in df.columns:
                # Use specific type from notebook if available, otherwise default (e.g., object)
                dtype_mapping[col_name] = notebook_item_dtypes.get(item_id, "object")
            # If the column doesn't exist (e.g., due to skipped merge), don't add it to dtype_mapping

        for col, dtype in dtype_mapping.items():
            # Check again if column exists before trying conversion
            if col in df.columns:
                try:
                    # Handle potential conversion issues, especially with Int64
                    if pd.api.types.is_integer_dtype(dtype) and df[col].isnull().any():
                        # Use pandas nullable integer type if NaNs are present
                        df[col] = df[col].astype(pd.Int64Dtype())
                    elif pd.api.types.is_float_dtype(dtype):
                        # Coerce errors during numeric conversion for floats
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                        df[col] = df[col].astype(dtype)  # Apply specific float type
                    elif dtype == "object":
                        # Convert potential numerical NaNs/NAs to actual None for object types
                        # Ensure it's string first, then replace various null representations
                        df[col] = (
                            df[col]
                            .astype(str)
                            .replace({"nan": None, "<NA>": None, "None": None})
                        )
                    else:
                        # For other types, attempt direct conversion
                        df[col] = df[col].astype(dtype)

                except Exception as e:
                    logging.warning(
                        f"Could not convert column '{col}' to {dtype}: {e}. Attempting safe conversion."
                    )
                    # Fallback conversions
                    try:
                        if pd.api.types.is_integer_dtype(dtype):
                            # Try converting to float first, then nullable Int
                            df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                                pd.Int64Dtype()
                            )
                        elif pd.api.types.is_float_dtype(dtype):
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                        elif dtype == "object":
                            # Ensure conversion to string and replace null representations
                            df[col] = (
                                df[col]
                                .astype(str)
                                .replace({"nan": None, "<NA>": None, "None": None})
                            )
                        else:
                            logging.error(
                                f"Unhandled conversion error for column '{col}' to {dtype}."
                            )
                            # Keep column as is or handle differently
                    except Exception as fallback_e:
                        logging.error(
                            f"Fallback conversion failed for column '{col}' to {dtype}: {fallback_e}"
                        )

        logging.info("Data type conversion complete.")
        # Optional: print dtypes for verification
        # logging.info(f"Final dtypes:\n{df.dtypes}")
        return df

    def merge_and_clean(
        self,
        anomalies_df: pd.DataFrame,
        users_df: Optional[pd.DataFrame] = None,
        ponderado_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Combina y limpia los datos de anomalías, usuarios y ponderaciones.

        Returns:
            pd.DataFrame: DataFrame consolidado y limpio listo para análisis.
        """

        self.logger.info("Iniciando procesamiento de datos...")

        # Guardar los DataFrames de entrada
        self.df_anomalies = anomalies_df
        self.df_users = users_df
        self.df_ponderado = ponderado_df

        cols_anomalies = [
            "Orden",
            "Usuario",
            "Ejecucion",
            "Codigo",
            "Descripcion",
            "Motivo",
            "kWh Rec",
            "Factor",
            "Zona",
        ]
        df_anomalies = self.df_anomalies[cols_anomalies].copy()

        df_expanded = df_anomalies.assign(
            Codigo=df_anomalies["Codigo"].astype(str).str.split("/")
        ).explode("Codigo")
        df_expanded["Codigo"] = df_expanded["Codigo"].str.strip()

        df_merge_anom_pond = pd.merge(
            df_expanded,
            self.df_ponderado[["Item", "id", "Nombre", "puntaje", "evaluacion"]].copy(),
            how="left",
            left_on="Codigo",
            right_on="Item",
        )
        df_merge_anom_pond.drop(columns=["Item"], inplace=True)

        columnas_deseadas = [
            "PRODUCTO",
            "AREA",
            "PLAN_COMERCIAL",
            "TRAFO_OPEN",
            "FASES",
            "KVA",
            "LATI_USU",
            "LONG_USU",
        ]
        df_users = self.df_users[columnas_deseadas].copy()

        df_merge_anom_pond_us = pd.merge(
            df_merge_anom_pond,
            df_users,
            left_on="Usuario",
            right_on="PRODUCTO",
            how="left",
        )

        columnas_deseadas = [
            "Orden",
            "Usuario",
            "Ejecucion",
            "Codigo",
            "Descripcion",
            "Motivo",
            "kWh Rec",
            "Factor",
            "id",
            "Nombre",
            "Factor",
            "AREA",
            "PLAN_COMERCIAL",
            "TRAFO_OPEN",
            "FASES",
            "KVA",
            "LATI_USU",
            "LONG_USU",
            "puntaje",
            "evaluacion",
            "Zona",
        ]
        df_final = df_merge_anom_pond_us[columnas_deseadas].copy()
        df_final = df_final.drop_duplicates()
        # Guardamos el resultado para su uso posterior en el pipeline
        self.processed_data = df_final

        if self.logger:
            self.logger.info(
                "Merge y limpieza completados. Filas resultantes: %d", len(df_final)
            )

        return self.processed_data

    def process_data(
        self,
        anomalies_df: pd.DataFrame,
        users_df: Optional[pd.DataFrame] = None,
        plain_df: Optional[pd.DataFrame] = None,
        uid_conversion_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Procesa los datos de anomalías combinándolos con información de usuarios y otros datos relacionados.

        Args:
            anomalies_df: DataFrame con datos de anomalías
            users_df: DataFrame opcional con datos de usuarios
            plain_df: DataFrame opcional con datos planos relacionados
            uid_conversion_df: DataFrame opcional con mapeo de IDs

        Returns:
            DataFrame procesado con todos los datos combinados
        """
        self.logger.info("Iniciando procesamiento de datos...")

        # Guardar los DataFrames de entrada
        self.df_anomalies = anomalies_df
        self.df_users = users_df
        self.df_plain = plain_df
        self.df_uid_conversion = uid_conversion_df

        # --- Step 1: Prepare initial data (Zonas and ODT Number) ---
        logging.info("Step 1: Preparing initial zone and ODT number data...")

        df_zonas = self.df_plain[self.df_plain["id"] == self.ZONA_ID].copy()
        df_odt_number = self.df_plain[self.df_plain["id"] == self.ODT_NUMBER_ID].copy()

        # Remove duplicates from odt_number before mapping
        df_odt_number = df_odt_number.drop_duplicates(subset="odt")
        logging.info(
            f"Unique ODT numbers (id={self.ODT_NUMBER_ID}): {len(df_odt_number)}"
        )

        # --- Step 2: Map ODT Number (item_288) onto Zone data ---
        logging.info("Step 2: Mapping ODT number (item_288)...")
        odt_map = df_odt_number.set_index("odt")["value"]
        df_zonas["item_288"] = df_zonas["odt"].map(odt_map)

        # Drop rows where mapping failed (NaNs in item_288)
        initial_rows = len(df_zonas)
        df_zonas.dropna(subset=["item_288"], inplace=True)
        dropped_rows = initial_rows - len(df_zonas)
        logging.info(f"Dropped {dropped_rows} rows due to missing item_288 mapping.")
        logging.info(f"DataFrame size after item_288 mapping: {len(df_zonas)} rows.")

        # Ensure item_288 is integer for merging
        try:
            # Attempt direct conversion first
            df_zonas["item_288"] = df_zonas["item_288"].astype(int)
        except (ValueError, TypeError) as e:
            logging.warning(
                f"Could not directly convert 'item_288' to int after mapping: {e}. Coercing to numeric."
            )
            # Coerce to numeric, errors='coerce' will turn non-numeric into NaN
            df_zonas["item_288"] = pd.to_numeric(df_zonas["item_288"], errors="coerce")
            # Drop rows that became NaN after coercion
            rows_before_coerce_drop = len(df_zonas)
            df_zonas.dropna(subset=["item_288"], inplace=True)
            rows_after_coerce_drop = len(df_zonas)
            if rows_before_coerce_drop > rows_after_coerce_drop:
                logging.warning(
                    f"Dropped {rows_before_coerce_drop - rows_after_coerce_drop} additional rows due to non-numeric 'item_288' values."
                )
            # Now convert to nullable integer type Int64 to handle potential NaNs if any remained (shouldn't if dropna worked)
            # Or convert directly to int if certain no NaNs remain
            df_zonas["item_288"] = df_zonas["item_288"].astype(
                int
            )  # or pd.Int64Dtype() if NaNs are possible

        # --- Step 3: Merge with User Data ---
        logging.info("Step 3: Merging with user data...")

        # Select necessary columns from df_zonas before merge
        # Make sure required merge key 'item_288' exists and is correct type
        if "item_288" not in df_zonas.columns:
            raise KeyError("'item_288' column missing before merging with user data.")
        if not pd.api.types.is_integer_dtype(df_zonas["item_288"]):
            logging.warning(
                "Attempting merge while 'item_288' is not integer. Converting."
            )
            # Assuming PRODUCTO in user data is numeric or can be coerced
            try:
                self.df_users["PRODUCTO"] = pd.to_numeric(
                    self.df_users["PRODUCTO"], errors="coerce"
                )
                self.df_users.dropna(
                    subset=["PRODUCTO"], inplace=True
                )  # Drop users with non-numeric PRODUCTO
                self.df_users["PRODUCTO"] = self.df_users["PRODUCTO"].astype(int)
                df_zonas["item_288"] = df_zonas["item_288"].astype(
                    int
                )  # Ensure left key is also int
            except Exception as e:
                raise TypeError(
                    f"Failed to ensure merge keys ('item_288', 'PRODUCTO') are compatible integers: {e}"
                )

        # Select columns from user data, ensure PRODUCTO exists
        user_cols = [
            "PRODUCTO",
            "AREA",
            "PLAN_COMERCIAL",
            "NIVEL",
            "LATI_USU",
            "LONG_USU",
            "ZONA",
        ]
        missing_user_cols = [col for col in user_cols if col not in self.df_users.columns]
        if missing_user_cols:
            raise KeyError(f"Missing required columns in user data: {missing_user_cols}")

        df_merged_users = pd.merge(
            df_zonas[["odt", "item_288"]],  # Include only necessary columns from left
            self.df_users[user_cols],  # Select required columns from right
            left_on="item_288",
            right_on="PRODUCTO",
            how="inner",  # As per notebook logic
        )
        # Drop the redundant PRODUCTO column from the merge
        df_merged_users = df_merged_users.drop(columns=["PRODUCTO"])
        logging.info(
            f"DataFrame size after user data merge: {len(df_merged_users)} rows."
        )

        # --- Step 4: Merge with UID Conversion Data ---
        logging.info("Step 4: Merging with UID conversion data...")

        # Ensure merge keys 'odt' and 'uid' exist and are compatible
        if "odt" not in df_merged_users.columns:
            raise KeyError(
                "'odt' column missing before merging with UID conversion data."
            )
        uid_cols = ["uid", "orden"]
        missing_uid_cols = [
            col for col in uid_cols if col not in self.df_uid_conversion.columns
        ]
        if missing_uid_cols:
            raise KeyError(
                f"Missing required columns in UID conversion data: {missing_uid_cols}"
            )

        # Optional: Check/convert types if necessary (e.g., if 'odt' or 'uid' might be strings)
        # Assuming they are numeric based on notebook context
        try:
            df_merged_users["odt"] = pd.to_numeric(
                df_merged_users["odt"], errors="coerce"
            ).astype(pd.Int64Dtype())
            self.df_uid_conversion["uid"] = pd.to_numeric(
                self.df_uid_conversion["uid"], errors="coerce"
            ).astype(pd.Int64Dtype())
            # Drop NaNs that might result from coercion if needed
            df_merged_users.dropna(subset=["odt"], inplace=True)
            self.df_uid_conversion.dropna(subset=["uid"], inplace=True)
        except Exception as e:
            logging.warning(
                f"Could not ensure 'odt' and 'uid' are numeric types for merge: {e}"
            )

        df_merged_uid = pd.merge(
            df_merged_users,
            self.df_uid_conversion[uid_cols],  # Select only needed columns
            left_on="odt",
            right_on="uid",
            how="inner",  # As per notebook logic
        )
        # Drop the redundant uid column
        df_merged_uid = df_merged_uid.drop(columns=["uid"])
        logging.info(
            f"DataFrame size after UID conversion merge: {len(df_merged_uid)} rows."
        )
        # Check if 'orden' column exists after merge
        if "orden" not in df_merged_uid.columns:
            logging.warning(
                "'orden' column not found after UID merge. Anomalies merge might fail or produce no results."
            )

        # --- Step 5: Process and Merge with Anomalies Data ---
        logging.info("Step 5: Processing and merging with anomalies data...")
        df_anomalies_processed = self._process_anomalies()

        # Check if 'orden' exists in the left df and 'Orden' in the right before merging
        if "orden" not in df_merged_uid.columns:
            logging.warning(
                "Skipping anomalies merge because 'orden' column is missing in the main dataframe."
            )
            # Add an empty 'Descripcion' column if it's expected later
            if "Descripcion" not in df_merged_uid.columns:
                df_merged_uid["Descripcion"] = None
            df_merged_anomalies = df_merged_uid  # Continue with the dataframe as is
        elif (
            df_anomalies_processed.empty or "Orden" not in df_anomalies_processed.columns
        ):
            logging.warning(
                "Skipping anomalies merge because processed anomalies data is empty or missing 'Orden' column."
            )
            if "Descripcion" not in df_merged_uid.columns:
                df_merged_uid["Descripcion"] = None
            df_merged_anomalies = df_merged_uid
        else:
            # Convert merge keys to compatible types (e.g., both int or both str)
            try:
                # Try converting both to Int64 (nullable integer)
                df_merged_uid["orden"] = pd.to_numeric(
                    df_merged_uid["orden"], errors="coerce"
                ).astype(pd.Int64Dtype())
                df_anomalies_processed["Orden"] = pd.to_numeric(
                    df_anomalies_processed["Orden"], errors="coerce"
                ).astype(pd.Int64Dtype())

                # Drop rows where conversion failed (became NaT/NaN)
                df_merged_uid.dropna(subset=["orden"], inplace=True)
                df_anomalies_processed.dropna(subset=["Orden"], inplace=True)

                logging.info("Merging anomalies using integer keys ('orden', 'Orden').")
                df_merged_anomalies = pd.merge(
                    df_merged_uid,
                    df_anomalies_processed,
                    left_on="orden",
                    right_on="Orden",
                    how="left",
                )
            except Exception as e_int:
                logging.warning(
                    f"Integer merge failed for anomalies ({e_int}). Trying string merge."
                )
                try:
                    # Fallback to string merge
                    df_merged_uid["orden_str"] = df_merged_uid["orden"].astype(str)
                    df_anomalies_processed["Orden_str"] = df_anomalies_processed[
                        "Orden"
                    ].astype(str)

                    logging.info(
                        "Merging anomalies using string keys ('orden_str', 'Orden_str')."
                    )
                    df_merged_anomalies = pd.merge(
                        df_merged_uid,
                        df_anomalies_processed,
                        left_on="orden_str",
                        right_on="Orden_str",
                        how="left",
                    )
                    # Drop temporary string columns
                    df_merged_anomalies.drop(
                        columns=["orden_str"], inplace=True, errors="ignore"
                    )
                    if "Orden_str" in df_merged_anomalies.columns:
                        df_merged_anomalies.drop(
                            columns=["Orden_str"], inplace=True
                        )  # Might not be present after left merge

                except Exception as e_str:
                    logging.error(
                        f"String merge also failed for anomalies ({e_str}). Skipping merge."
                    )
                    if "Descripcion" not in df_merged_uid.columns:
                        df_merged_uid["Descripcion"] = None
                    df_merged_anomalies = df_merged_uid  # Continue without merge

            # Drop the redundant Orden column if it exists
            if "Orden" in df_merged_anomalies.columns:
                df_merged_anomalies = df_merged_anomalies.drop(columns=["Orden"])

            # # Handle NaN Descriptions that might result from left join
            # if 'Descripcion' in df_merged_anomalies.columns:
            #      # *** FIX: Use value=None keyword argument ***
            #      df_merged_anomalies['Descripcion'].fillna(value=None, inplace=True) # Use None instead of NaN string
            # else:
            #      # Ensure the column exists even if the merge didn't happen or add it
            #      df_merged_anomalies['Descripcion'] = None

            logging.info(
                f"DataFrame size after anomalies merge step: {len(df_merged_anomalies)} rows."
            )

        # --- Step 6: Select Base Columns and Merge Items ---
        logging.info("Step 6: Selecting base columns and merging dynamic items...")
        # Define base columns expected before item merge
        base_columns = [
            "item_288",
            "PLAN_COMERCIAL",
            "NIVEL",
            "LATI_USU",
            "LONG_USU",
            "AREA",
            "ZONA",
            "Descripcion",
            "odt",
            "orden",
        ]
        # Filter list to include only columns that actually exist in the dataframe at this stage
        base_columns_existing = [
            col for col in base_columns if col in df_merged_anomalies.columns
        ]
        logging.info(f"Using existing base columns: {base_columns_existing}")
        df_base_for_items = df_merged_anomalies[base_columns_existing].copy()

        df_final_items = self._merge_items(df_base_for_items)
        logging.info(
            f"DataFrame size after merging all items: {len(df_final_items)} rows."
        )

        # --- Step 7: Final Cleanup and Type Conversion ---
        logging.info("Step 7: Final cleanup and type application...")
        # Replace potential nulls in 'Descripcion' again after merges might have reintroduced them
        if "Descripcion" in df_final_items.columns:
            # Ensure consistent None representation
            df_final_items["Descripcion"] = (
                df_final_items["Descripcion"]
                .astype(str)
                .replace({"nan": None, "<NA>": None, "None": None})
            )

        # Apply final data types
        self.processed_data = self._apply_data_types(df_final_items)
        # Verificar si existe la columna 'año' en el DataFrame de anomalías
        if "año" not in self.processed_data.columns:
            self.logger.warning(
                "La columna 'año' no existe en el DataFrame de anomalías. Intentando extraer año de 'Ejecucion'..."
            )

            # Si aún no tenemos la columna año, crear la columna con el valor de target_year
            self.logger.warning(
                f"Usando el año objetivo {self.target_year} para todos los registros."
            )
            self.processed_data["año"] = self.target_year

        logging.info("Data processing pipeline finished.")
        logging.info(f"Final dataset dimensions: {self.processed_data.shape}")
        self.logger.info("Procesamiento de datos completado")
        return self.processed_data

    def generate_report(self) -> Dict:
        """
        Genera un informe detallado del procesamiento de datos.

        Returns:
            Diccionario con estadísticas y metadatos del procesamiento
        """
        self.logger.info("Generando reporte de procesamiento...")

        report = {
            "timestamp": datetime.now().isoformat(),
            "target_year": self.target_year,
        }

        # Estadísticas de los datos originales
        input_stats = {
            "anomalies_df": {
                "rows": len(self.df_anomalies) if self.df_anomalies is not None else 0,
                "columns": len(self.df_anomalies.columns)
                if self.df_anomalies is not None
                else 0,
            },
            "users_df": {
                "rows": len(self.df_users) if self.df_users is not None else 0,
                "columns": len(self.df_users.columns) if self.df_users is not None else 0,
            },
            "plain_df": {
                "rows": len(self.df_plain) if self.df_plain is not None else 0,
                "columns": len(self.df_plain.columns) if self.df_plain is not None else 0,
            },
            "uid_conversion_df": {
                "rows": len(self.df_uid_conversion)
                if self.df_uid_conversion is not None
                else 0,
                "columns": len(self.df_uid_conversion.columns)
                if self.df_uid_conversion is not None
                else 0,
            },
        }
        report["input_statistics"] = input_stats

        # Estadísticas de los datos procesados
        if self.processed_data is not None and not self.processed_data.empty:
            processed_stats = {
                "rows": len(self.processed_data),
                "columns": len(self.processed_data.columns),
                "missing_values": self.processed_data.isna().sum().sum(),
                "missing_percentage": round(
                    (
                        self.processed_data.isna().sum().sum()
                        / (len(self.processed_data) * len(self.processed_data.columns))
                    )
                    * 100,
                    2,
                ),
            }

            # Información sobre tipos de datos
            dtype_counts = {
                str(dtype): count
                for dtype, count in self.processed_data.dtypes.value_counts().items()
            }
            processed_stats["data_types"] = dtype_counts

            # Lista de columnas por tipo
            dtype_columns = {}
            for dtype in set(self.processed_data.dtypes):
                cols = list(self.processed_data.select_dtypes(include=[dtype]).columns)
                if cols:
                    dtype_columns[str(dtype)] = cols
            processed_stats["columns_by_type"] = dtype_columns

            # Número de ítems encontrados
            processed_stats["item_columns_count"] = len(self.item_ids)

            report["processed_statistics"] = processed_stats

        # Guardar el reporte
        self.report = report
        self.logger.info("Reporte de procesamiento generado")

        return report

    def get_processed_data(self) -> pd.DataFrame:
        """
        Devuelve los datos procesados.

        Returns:
            DataFrame con los datos procesados
        """
        if self.processed_data is None:
            self.logger.warning("No hay datos procesados disponibles")
            return pd.DataFrame()

        return self.processed_data

    def get_report(self) -> Dict:
        """
        Devuelve el reporte generado durante el procesamiento.

        Returns:
            Diccionario con el reporte completo
        """
        return self.report
