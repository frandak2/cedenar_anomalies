import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account  # Para cargar credenciales explícitamente

from cedenar_anomalies.utils.paths import (  # Asumiendo que esta función existe y es correcta
    data_interim_dir,
    data_raw_dir,
)

# --- Configuración ---
ARCHIVO_CSV = data_interim_dir("dataset_inference.csv")
ARCHIVO_CREDENCIALES_JSON = data_raw_dir("proyecto-ia.json")

# --- Configuración de BigQuery ---
PROJECT_ID = "proyecto-ia-462422"  # Extraído de la imagen
DATASET_ID = "Datos_IA_LK"  # Extraído de la imagen
TABLE_NAME = "Datos_Inference"  # Nombre de la tabla destino en BigQuery (igual que el dataset para este ejemplo)
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# --- 1. Autenticación y creación del cliente de BigQuery ---
try:
    credentials = service_account.Credentials.from_service_account_file(
        ARCHIVO_CREDENCIALES_JSON,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print(f"Cliente de BigQuery inicializado para el proyecto '{PROJECT_ID}'.")
except Exception as e:
    print(f"Error de autenticación o al crear el cliente de BigQuery: {e}")
    exit()

# --- 2. Leer los datos del archivo CSV usando Pandas ---
try:
    df = pd.read_csv(ARCHIVO_CSV, encoding="utf-8")
    print(
        f"Datos leídos de '{ARCHIVO_CSV}'. Filas: {len(df)}, Columnas: {len(df.columns)}"
    )

    if df.empty:
        print(f"El archivo CSV '{ARCHIVO_CSV}' está vacío. No hay datos para subir.")
        exit()

    # --- Renombrar cluster_id a Cluster ---
    if "cluster_id" in df.columns:
        df.rename(columns={"cluster_id": "Cluster"}, inplace=True)
        print("Columna 'cluster_id' renombrada a 'Cluster'.")
    else:
        print("Advertencia: Columna 'cluster_id' no encontrada en el CSV para renombrar.")

    if "kWh Rec" in df.columns:
        df.rename(columns={"kWh Rec": "kWh_Rec"}, inplace=True)
        print("Columna 'kWh Rec' renombrada a 'kWh_Rec'.")
    else:
        print("Advertencia: Columna 'kWh Rec' no encontrada en el CSV para renombrar.")

    # --- Preprocesamiento de datos y conversión de tipos en Pandas ---

    # Columnas esperadas y sus tipos BQ (basado en tu imagen)
    # Hacer una copia para no modificar el df original innecesariamente hasta el final
    df_procesado = pd.DataFrame()

    # STRING Types
    string_cols = ["AREA", "Cluster", "Nombre", "PLAN_COMERCIAL", "ZONA"]
    for col in string_cols:
        if col in df.columns:
            df_procesado[col] = (
                df[col].astype(str).replace("nan", None)
            )  # Convertir 'nan' string a None
        else:
            print(f"Advertencia: Columna '{col}' no encontrada en el CSV. Se omitirá.")

    # INTEGER Types (usando Int64Dtype para soportar NaNs como pd.NA)
    integer_cols = ["puntaje", "Usuario"]
    for col in integer_cols:
        if col in df.columns:
            # Primero convertir a numérico, errores a NaN
            numeric_col = pd.to_numeric(df[col], errors="coerce")
            # Luego convertir a Int64Dtype, que maneja NaN como pd.NA
            df_procesado[col] = numeric_col.astype(pd.Int64Dtype())
        else:
            print(f"Advertencia: Columna '{col}' no encontrada en el CSV. Se omitirá.")

    # NUMERIC Types (FLOAT64 en Pandas, FLOAT en BQ)
    # 'Usuario' lo trato como FLOAT por si tiene decimales, si es INTEGER, BQ lo manejará.
    # O puedes usar pd.Int64Dtype() para enteros con NaNs si es necesario.
    float_cols = [
        "kWh_Rec",
        "LATI_USU",
        "LONG_USU",
        "puntaje_1",
        "puntaje_2",
        "puntaje_3",
        "puntaje_4",
        "puntaje_5",
    ]
    for col in float_cols:
        if col in df.columns:
            df_procesado[col] = pd.to_numeric(
                df[col], errors="coerce"
            )  # 'coerce' convierte errores a NaT/NaN
        else:
            print(f"Advertencia: Columna '{col}' no encontrada en el CSV. Se omitirá.")

    # DATE Type for 'Ejecucion'
    if "Ejecucion" in df.columns:
        # Intenta convertir a datetime, errores se convertirán a NaT (Not a Time)
        # BigQuery espera objetos date, no datetime, para el tipo DATE.
        df_procesado["Ejecucion"] = pd.to_datetime(
            df["Ejecucion"], errors="coerce"
        ).dt.date
    else:
        print("Advertencia: Columna 'Ejecucion' no encontrada en el CSV. Se omitirá.")

    # Asegurarnos de que todas las columnas que BigQuery espera estén en el DataFrame
    # y en el orden correcto para el esquema definido (aunque BQ carga por nombre).
    # Este es el orden de tu imagen, que usaremos para el schema.
    column_order_for_bq = [
        "AREA",
        "Cluster",
        "Ejecucion",
        "kWh_Rec",
        "LATI_USU",
        "LONG_USU",
        "Nombre",
        "PLAN_COMERCIAL",
        "puntaje",
        "puntaje_1",
        "puntaje_2",
        "puntaje_3",
        "puntaje_4",
        "puntaje_5",
        "Usuario",
        "ZONA",
    ]

    # Reordenar y seleccionar solo las columnas necesarias
    # Si alguna columna falta en df_procesado, se añadirá con None/NaN
    df_para_subir = pd.DataFrame()
    for col_name in column_order_for_bq:
        if col_name in df_procesado.columns:
            df_para_subir[col_name] = df_procesado[col_name]
        else:
            # Si una columna definida en column_order_for_bq no se pudo crear/encontrar
            # la añadimos con Nones para que el esquema de BQ no falle.
            print(
                f"Info: Columna '{col_name}' no presente en el DataFrame procesado, se añadirá como Nones."
            )
            df_para_subir[col_name] = None


except FileNotFoundError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV}' no fue encontrado.")
    exit()
except pd.errors.EmptyDataError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV}' está vacío.")
    exit()
except Exception as e:
    print(f"Error procesando los datos con Pandas: {e}")
    import traceback

    traceback.print_exc()
    exit()

# --- 3. Definir el esquema de BigQuery y ejecutar el job de carga ---
try:
    # Definir el esquema explícitamente basado en tu imagen y el preprocesamiento
    schema = [
        bigquery.SchemaField("AREA", "STRING", mode="NULLABLE"),
        # bigquery.SchemaField("Cluster", "STRING", mode="NULLABLE"), # Si el CSV tuviera Cluster directamente
        bigquery.SchemaField("Cluster", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Ejecucion", "DATE", mode="NULLABLE"),
        bigquery.SchemaField(
            "kWh_Rec", "FLOAT", mode="NULLABLE"
        ),  # O NUMERIC si prefieres
        bigquery.SchemaField("LATI_USU", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("LONG_USU", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("Nombre", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("PLAN_COMERCIAL", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("puntaje", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("puntaje_1", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("puntaje_2", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("puntaje_3", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("puntaje_4", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("puntaje_5", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField(
            "Usuario", "INTEGER", mode="NULLABLE"
        ),  # O INTEGER si estás seguro que no hay decimales
        bigquery.SchemaField("ZONA", "STRING", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # autodetect=False, # Ya no es necesario porque proveemos el schema
        # source_format no es necesario para load_table_from_dataframe si los tipos están bien en el DF
    )

    print(f"Subiendo {len(df_para_subir)} filas a la tabla '{TABLE_ID}' en BigQuery...")
    # Asegurarse que df_para_subir tiene las columnas en el mismo orden que el schema
    # aunque BigQuery carga por nombre de columna.
    job = client.load_table_from_dataframe(
        df_para_subir[column_order_for_bq],  # Selecciona y ordena las columnas
        TABLE_ID,
        job_config=job_config,
    )
    job.result()  # Esperar a que el job de carga se complete

    table = client.get_table(TABLE_ID)
    print(
        f"¡Datos subidos exitosamente! Se cargaron {table.num_rows} filas a la tabla '{TABLE_ID}'."
    )
    print(
        f"Puedes ver la tabla aquí: https://console.cloud.google.com/bigquery?project={PROJECT_ID}&p={PROJECT_ID}&d={DATASET_ID}&t={TABLE_NAME}&page=table"
    )

except Exception as e:
    print(f"Error al subir datos a BigQuery: {e}")
    if hasattr(e, "errors") and e.errors:
        print("Detalles del error:")
        for error_detail in e.errors:
            print(error_detail)
    import traceback

    traceback.print_exc()
