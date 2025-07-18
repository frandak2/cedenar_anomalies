import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account  # Para cargar credenciales explícitamente

from cedenar_anomalies.utils.paths import (  # Asumiendo que esta función existe y es correcta
    data_processed_dir,
    data_raw_dir,
)

# --- Configuración ---
ARCHIVO_CSV = data_processed_dir("anomalia_data.csv")
ARCHIVO_CREDENCIALES_JSON = data_raw_dir("proyecto-ia.json")

# --- Configuración de BigQuery ---
PROJECT_ID = "proyecto-ia-462422"  # Extraído de la imagen
DATASET_ID = "Datos_IA_LK"  # Extraído de la imagen
TABLE_NAME = "datos_anomalia"  # Nombre de la tabla destino en BigQuery (igual que el dataset para este ejemplo)
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

    # --- Preprocesamiento de datos y conversión de tipos en Pandas ---

    # Columnas esperadas y sus tipos BQ (basado en tu imagen)
    # Hacer una copia para no modificar el df original innecesariamente hasta el final
    df_procesado = pd.DataFrame()

    # STRING Types
    string_cols = [
        "AREA",
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
        "PLAN_COMERCIAL",
        "Descripcion",
        "reincidente",
        "Anomalia_conf",
        "ZONA",
        "id",
    ]
    for col in string_cols:
        if col in df.columns:
            df_procesado[col] = (
                df[col].astype(str).replace("nan", None)
            )  # Convertir 'nan' string a None
        else:
            print(f"Advertencia: Columna '{col}' no encontrada en el CSV. Se omitirá.")

    # INTEGER Types (usando Int64Dtype para soportar NaNs como pd.NA)
    integer_cols = ["NIVEL", "año", "item_288", "odt", "orden"]
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
    float_cols = ["LATI_USU", "LONG_USU"]
    for col in float_cols:
        if col in df.columns:
            df_procesado[col] = pd.to_numeric(
                df[col], errors="coerce"
            )  # 'coerce' convierte errores a NaT/NaN
        else:
            print(f"Advertencia: Columna '{col}' no encontrada en el CSV. Se omitirá.")

    # DATE Type for 'Ejecucion'
    if "fecha_creacion" in df.columns:
        # Intenta convertir a datetime, errores se convertirán a NaT (Not a Time)
        # BigQuery espera objetos date, no datetime, para el tipo DATE.
        df_procesado["fecha_creacion"] = pd.to_datetime(
            df["fecha_creacion"], errors="coerce"
        ).dt.date
    else:
        print("Advertencia: Columna 'Ejecucion' no encontrada en el CSV. Se omitirá.")

    # Asegurarnos de que todas las columnas que BigQuery espera estén en el DataFrame
    # y en el orden correcto para el esquema definido (aunque BQ carga por nombre).
    # Este es el orden de tu imagen, que usaremos para el schema.
    column_order_for_bq = [
        "item_288",
        "año",
        "Anomalia_conf",
        "AREA",
        "Descripcion",
        "ZONA",
        "item_237",
        "item_68",
        "item_74",
        "item_248",
        "item_23",
        "item_24",
        "item_108",
        "item_603",
        "item_597",
        "item_601",
        "LATI_USU",
        "LONG_USU",
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
    # Paso 1: Crear un mapeo de nombre de columna a tipo de dato para búsqueda rápida
    column_type_mapping = {}
    for col in string_cols:
        column_type_mapping[col] = "STRING"
    for col in integer_cols:
        column_type_mapping[col] = "INTEGER"
    for col in float_cols:
        column_type_mapping[col] = "FLOAT"

    # (Opcional) Puedes añadir otros tipos aquí si los necesitas para columnas
    # que no están en string_cols, integer_cols, o float_cols pero sí en column_order_for_bq
    # Ejemplo:
    # column_type_mapping["Ejecucion"] = "DATE"
    # column_type_mapping["kWh_Rec"] = "FLOAT"
    # ...etc.

    # Paso 2: Construir el schema en el orden especificado
    schema = []
    for col_name in column_order_for_bq:
        if col_name in column_type_mapping:
            col_type = column_type_mapping[col_name]
            schema.append(bigquery.SchemaField(col_name, col_type, mode="NULLABLE"))
        else:
            # Manejar el caso donde una columna en column_order_for_bq no tiene un tipo definido
            # Puedes decidir qué hacer: omitirla, asignarle un tipo por defecto, o lanzar un error.
            print(
                f"Advertencia: La columna '{col_name}' está en 'column_order_for_bq' pero no tiene un tipo definido en las listas _cols. Se omitirá."
            )
            # O podrías hacer:
            # raise ValueError(f"La columna '{col_name}' no tiene un tipo definido.")
            # O asignar un tipo por defecto si es apropiado:
            # schema.append(bigquery.SchemaField(col_name, "STRING", mode="NULLABLE")) # Ejemplo con STRING por defecto

    # (Opcional) Imprimir el esquema para verificar
    print("Schema generado:")

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
