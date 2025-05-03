import logging

# Use relative import since data_processor is in the same package directory
from data_processor import DataProcessor  # Use relative import

# Import path functions - Only import if other utils from paths are needed here.
# Removed unused imports: data_processed_dir, data_raw_dir as they are not
# directly used in this script. DataProcessor handles its own path needs.
try:
    # Example: If you needed 'project_dir' from paths.py here, you'd import it:
    # from cedenar_anomalies.utils.paths import project_dir
    pass  # No direct path imports needed in this file based on current usage
except ImportError:
    logging.error("Could not import from cedenar_anomalies.utils.paths.")
    logging.error(
        "Please ensure the 'cedenar_anomalies' package is installed correctly "
        "and the 'utils/paths.py' file exists."
    )
    # Optionally, provide fallbacks or exit if utils are critical here
    import sys

    sys.exit(1)  # Exit if utils are absolutely necessary for make_dataset itself


# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Main function to configure and run the Zentry data processing.
    """
    # --- Configuration Variables ---
    # Define input filenames (relative to data_raw_dir)
    plain_data_filename = "plain3.csv"
    user_data_filename = "cedenar_data.xlsx"
    # Line 105 was too long - shortened variable name example (adjust as needed)
    uid_conv_filename = "conversion uid orden.xlsx"
    anomalies_filename = "anomalias 2022 23 y 24.xlsx"

    # Define Processing Parameters
    target_year = 2023
    # Define the list of item IDs
    # Line 124 was too long - example of splitting the list
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
    # --- End Configuration ---

    logging.info("Starting data processing with configured parameters...")
    logging.info(f"  Target Year: {target_year}")
    logging.info(f"  Item IDs: {item_ids}")
    logging.info(f"  Plain Data File: {plain_data_filename}")
    logging.info(f"  User Data File: {user_data_filename}")
    logging.info(f"  UID Conversion File: {uid_conv_filename}")
    logging.info(f"  Anomalies File: {anomalies_filename}")


    try:
        # --- Instantiate and Run Processor ---
        processor = DataProcessor(
            plain_filename=plain_data_filename,
            user_filename=user_data_filename,
            uid_conversion_filename=uid_conv_filename,  # Use shortened name
            anomalies_filename=anomalies_filename,
            target_year=target_year,
            item_ids=item_ids,
        )
        # Execute the full workflow
        processed_df = processor.run()

        if processed_df is not None:
            logging.info(
                "Processing finished successfully. "
                f"Final DataFrame shape: {processed_df.shape}"
            )
        else:
            logging.warning(
                "Processing finished, but the final DataFrame was not returned."
            )

    except FileNotFoundError as e:
        logging.error(
            f"Input file not found: {e}. Please check filenames and ensure "
            "they exist in the raw data directory."
        )
        # No sys.exit needed, just log and end.
    except KeyError as e:
        logging.error(
            f"Missing expected column during processing: {e}. Check input file structure."
        )
    # Line 140: Consider catching more specific exceptions if possible
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during processing: {e}", exc_info=True
        )
        # No sys.exit needed.


if __name__ == "__main__":
    main()
