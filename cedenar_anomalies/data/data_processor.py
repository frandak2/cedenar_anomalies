import logging
from typing import List, Optional
import pandas as pd  # Keep pandas import

# Import path functions
from cedenar_anomalies.utils.paths import (
    data_processed_dir,
    data_raw_dir,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataProcessor:
    """
    Processes Zentry data to generate a consolidated dataset based on specified year and item IDs.

    Handles loading data from data_raw_dir, merging various sources, filtering based on year,
    dynamically adding item columns, and saving the results along with a report to data_processed_dir.
    """

    # Constants for specific IDs mentioned in the notebook
    ZONA_ID = 1
    ODT_NUMBER_ID = 288

    def __init__(
        self,
        plain_filename: str,
        user_filename: str,
        uid_conversion_filename: str,
        anomalies_filename: str,
        target_year: int,
        item_ids: List[int],
    ):
        """
        Initializes the DataProcessor.

        Args:
            plain_filename: Filename of the main data file in data_raw_dir (e.g., 'plain3.csv').
            user_filename: Filename of the user data file in data_raw_dir (e.g., 'cedenar_data.xlsx').
            uid_conversion_filename: Filename of the UID conversion file in data_raw_dir (e.g., 'conversion uid orden.xlsx').
            anomalies_filename: Filename of the anomalies file in data_raw_dir (e.g., 'anomalias 2022 23 y 24.xlsx').
            target_year: The year to filter the anomalies data.
            item_ids: A list of item IDs to merge into the final dataset.
        """
        self.plain_filename = plain_filename
        self.user_filename = user_filename
        self.uid_conversion_filename = uid_conversion_filename
        self.anomalies_filename = anomalies_filename
        self.target_year = target_year
        self.item_ids = item_ids

        # DataFrames to be loaded
        self.df_plain: Optional[pd.DataFrame] = None
        self.df_users: Optional[pd.DataFrame] = None
        self.df_uid_conversion: Optional[pd.DataFrame] = None
        self.df_anomalies: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None
        self.report: Optional[pd.DataFrame] = None

    def _load_data(self):
        """Loads all necessary data files from data_raw_dir into pandas DataFrames."""
        logging.info("Loading data...")
        try:
            plain_data_path = data_raw_dir(self.plain_filename)
            self.df_plain = pd.read_csv(plain_data_path, low_memory=False)
            logging.info(f"Loaded plain data from: {plain_data_path}")

            user_data_path = data_raw_dir(self.user_filename)
            self.df_users = pd.read_excel(user_data_path)
            logging.info(f"Loaded user data from: {user_data_path}")

            uid_conversion_path = data_raw_dir(self.uid_conversion_filename)
            self.df_uid_conversion = pd.read_excel(uid_conversion_path)
            logging.info(f"Loaded UID conversion data from: {uid_conversion_path}")

            anomalies_path = data_raw_dir(self.anomalies_filename)
            self.df_anomalies = pd.read_excel(anomalies_path)
            logging.info(f"Loaded anomalies data from: {anomalies_path}")

        except FileNotFoundError as e:
            logging.error(f"Error loading data: {e}. Ensure files exist in data_raw_dir.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during data loading: {e}")
            raise
        logging.info("Data loading complete.")

    def _extract_year_from_anomalies(self):
        """Extracts the year from the 'Ejecucion' column of the anomalies DataFrame."""
        if self.df_anomalies is None or "Ejecucion" not in self.df_anomalies.columns:
            logging.warning(
                "Anomalies DataFrame not loaded or 'Ejecucion' column missing."
            )
            return

        col = "Ejecucion"
        # Attempt conversion to datetime first
        try:
            execution_dt = pd.to_datetime(self.df_anomalies[col], errors="coerce")
            self.df_anomalies["año"] = execution_dt.dt.year
            # Handle cases where conversion failed (NaT)
            if self.df_anomalies["año"].isnull().any():
                logging.warning(
                    f"Some values in '{col}' could not be converted to datetime."
                )
                # Optional: Try string extraction for NaT rows if needed, though year filter later handles NaNs
                # mask_failed = execution_dt.isnull()
                # self.df_anomalies.loc[mask_failed, 'año'] = self.df_anomalies.loc[mask_failed, col].astype(str).str.extract(r'(\d{4})', expand=False)

        except Exception as e:
            logging.warning(
                f"Could not directly convert '{col}' to datetime ({e}). Trying string extraction."
            )
            # Fallback to string extraction if datetime conversion fails broadly
            self.df_anomalies["año"] = (
                self.df_anomalies[col].astype(str).str.extract(r"(\d{4})", expand=False)
            )

        # Convert the extracted year column to numeric, coercing errors
        self.df_anomalies["año"] = pd.to_numeric(
            self.df_anomalies["año"], errors="coerce"
        )
        logging.info("Extracted 'año' column from anomalies.")

    def _process_anomalies(self) -> pd.DataFrame:
        """Filters and cleans the anomalies data for the target year."""
        if self.df_anomalies is None:
            raise ValueError("Anomalies data not loaded.")

        logging.info(f"Processing anomalies for the year {self.target_year}...")
        self._extract_year_from_anomalies()

        # Filter by target year
        anom_filtered_year = self.df_anomalies[
            self.df_anomalies["año"] == self.target_year
        ].copy()
        logging.info(
            f"Found {len(anom_filtered_year)} anomaly records for {self.target_year}."
        )

        if anom_filtered_year.empty:
            logging.warning(f"No anomaly data found for the year {self.target_year}.")
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
        logging.info(f"Cleaned anomalies data size: {len(anom_cleaned)} rows.")

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

    def process(self):
        """Runs the full data processing pipeline."""
        # Load data if not already loaded
        if (
            self.df_plain is None
            or self.df_users is None
            or self.df_uid_conversion is None
            or self.df_anomalies is None
        ):
            self._load_data()

        if self.df_plain is None:
            raise ValueError("Failed to load plain data. Cannot proceed.")
        if self.df_users is None:
            raise ValueError("Failed to load user data. Cannot proceed.")
        if self.df_uid_conversion is None:
            raise ValueError("Failed to load UID conversion data. Cannot proceed.")
        if self.df_anomalies is None:
            raise ValueError("Failed to load anomalies data. Cannot proceed.")

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
        logging.info("Data processing pipeline finished.")
        logging.info(f"Final dataset dimensions: {self.processed_data.shape}")
        # logging.info(f"Final columns: {self.processed_data.columns.tolist()}") # Uncomment to see columns

    def generate_report(self) -> pd.DataFrame:
        """Generates a summary report of the processed data."""
        if self.processed_data is None:
            # Try running process() if data isn't ready
            logging.warning(
                "Processed data not found. Attempting to run process() before generating report."
            )
            try:
                self.process()
            except Exception as e:
                logging.error(f"Failed to run process() before generating report: {e}")
                raise ValueError(
                    "Data has not been processed successfully. Cannot generate report."
                )
            # Check again if processed_data exists after running process()
            if self.processed_data is None:
                raise ValueError(
                    "Data is still not available after running process(). Cannot generate report."
                )

        logging.info("Generating data summary report...")
        summary_list = []
        total_records = len(self.processed_data)

        if total_records == 0:
            logging.warning("Processed data is empty. Report will indicate zero records.")
            # Optionally return an empty report or a report indicating emptiness
            # return pd.DataFrame(columns=['Columna', 'Tipo de Dato', 'Registros Totales', 'Valores Únicos', 'Valores Nulos', '% Nulos', 'Estadísticas'])

        for column in self.processed_data.columns:
            col_data = self.processed_data[column]
            # Basic info calculation
            data_type = col_data.dtype
            unique_vals = col_data.nunique() if total_records > 0 else 0
            null_vals = col_data.isnull().sum() if total_records > 0 else 0
            null_pct = (null_vals / total_records * 100) if total_records > 0 else 0

            col_info = {
                "Columna": column,
                "Tipo de Dato": data_type,
                "Registros Totales": total_records,
                "Valores Únicos": unique_vals,
                "Valores Nulos": null_vals,
                "% Nulos": f"{null_pct:.2f}%",
                "Estadísticas": "",
            }

            # Calculate stats only if there's data
            if total_records > 0:
                if (
                    pd.api.types.is_numeric_dtype(col_data)
                    and not col_data.isnull().all()
                ):
                    # Ensure stats are calculated only on non-null values if appropriate, though agg usually handles this
                    stats = col_data.agg(
                        ["min", "max", "mean", "median", "std"]
                    ).to_dict()
                    col_info["Estadísticas"] = ", ".join(
                        [f"{k}: {v:.2f}" for k, v in stats.items() if pd.notnull(v)]
                    )
                elif (
                    pd.api.types.is_datetime64_any_dtype(col_data)
                    and not col_data.isnull().all()
                ):
                    stats = col_data.agg(["min", "max"]).to_dict()
                    col_info["Estadísticas"] = ", ".join(
                        [f"{k}: {v}" for k, v in stats.items() if pd.notnull(v)]
                    )
                elif (
                    col_data.dtype == "object" and not col_data.isnull().all()
                ):  # Object or other types
                    # Show top 5 most frequent values, handle potential errors if all are unique/null
                    try:
                        top_values = col_data.value_counts().nlargest(5).to_dict()
                        col_info["Estadísticas"] = "Top 5: " + ", ".join(
                            [f"{str(k)} ({v})" for k, v in top_values.items()]
                        )
                    except Exception as e:
                        logging.warning(
                            f"Could not get top values for column '{column}': {e}"
                        )
                        col_info["Estadísticas"] = "N/A"

                else:  # Fallback for other types or all-null columns
                    col_info["Estadísticas"] = "N/A"

            summary_list.append(col_info)

        self.report = pd.DataFrame(summary_list)
        logging.info("Report generation complete.")
        return self.report

    def save_results(self):
        """Saves the processed data and the report to CSV files in data_processed_dir."""
        if self.processed_data is None:
            logging.error("No processed data to save. Run process() first.")
            # Optionally try running process again, or just return
            return
            # try:
            #     logging.warning("Attempting to run process() before saving.")
            #     self.process()
            #     if self.processed_data is None: # Check again
            #          raise ValueError("Processing failed, cannot save.")
            # except Exception as e:
            #     logging.error(f"Failed to process data before saving: {e}")
            #     return

        if self.report is None:
            logging.warning("Report not generated. Generating report before saving.")
            try:
                self.generate_report()
            except Exception as e:
                logging.error(f"Failed to generate report before saving: {e}")
                # Decide whether to proceed saving only the data or stop
                # return # Stop if report generation failed

        # Define output filenames using the target year
        processed_filename = f"dataset_decantado_{self.target_year}.csv"
        report_filename = f"informe_columnas_{self.target_year}.csv"

        # Construct full paths using data_processed_dir
        processed_filepath = data_processed_dir(processed_filename)
        report_filepath = data_processed_dir(report_filename)

        try:
            # Ensure the processed directory exists (data_processed_dir function might handle this, but doesn't hurt to check)
            # os.makedirs(data_processed_dir(), exist_ok=True) # data_processed_dir() needs to return the path string

            logging.info(f"Saving processed data to: {processed_filepath}")
            self.processed_data.drop_duplicates().to_csv(
                processed_filepath, index=False, encoding="utf-8"
            )

            if self.report is not None:
                logging.info(f"Saving report to: {report_filepath}")
                self.report.to_csv(report_filepath, index=False, encoding="utf-8")
            else:
                logging.warning("Report DataFrame is missing, cannot save report.")

            logging.info("Results saved successfully.")

        except Exception as e:
            logging.error(f"Failed to save results: {e}")
            raise

    def run(self):
        """Executes the entire workflow: load, process, report, save."""
        try:
            self.process()  # Load and process data
            self.generate_report()  # Generate report based on processed data
            self.save_results()  # Save both data and report
            logging.info(
                f"Processing for year {self.target_year} completed successfully."
            )
            return self.processed_data  # Return the final DataFrame
        except Exception as e:
            logging.error(
                f"An error occurred during the run workflow: {e}", exc_info=True
            )  # Log traceback
            # Optionally, re-raise the exception if the caller needs to handle it
            raise  # Re-raise the exception after logging
