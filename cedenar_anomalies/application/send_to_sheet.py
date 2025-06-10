import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials # Importante para este método
from cedenar_anomalies.utils.paths import data_raw_dir, data_interim_dir

# --- Configuración ---
ARCHIVO_CSV = data_interim_dir('dataset_inference.csv')  # Nombre de tu archivo CSV
ARCHIVO_CSV_ERRORES = data_interim_dir('errores_inference.csv')  # Nombre de tu archivo CSV
NOMBRE_HOJA_GOOGLE = 'Datos_IA_LK' # El nombre de tu Google Sheet
NOMBRE_PESTANA_DEV = 'Dev'# Nombre de la pestaña (worksheet) donde se subirán los datos
NOMBRE_PESTANA_ERRORES = 'Errores'
ARCHIVO_CREDENCIALES_JSON = data_raw_dir('proyecto-ia-app-sheet.json') # Ruta a tu archivo JSON de credenciales

# Define los "scopes" (ámbitos de permiso) que tu script necesita.
# Para leer y escribir en hojas de cálculo y acceder a Drive (donde están las hojas)
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# --- 1. Autenticación con oauth2client ---
try:
    # Cargar las credenciales desde el archivo JSON
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        ARCHIVO_CREDENCIALES_JSON,
        SCOPES
    )
    # Autorizar al cliente de gspread con estas credenciales
    gc = gspread.authorize(creds)
    print("Autenticación exitosa con oauth2client.")
except Exception as e:
    print(f"Error de autenticación: {e}")
    print("Asegúrate de que el archivo de credenciales JSON es correcto y los scopes son adecuados.")
    exit()

# --- 2. Abrir la Hoja de Google y la pestaña (Worksheet) ---
try:
    sh = gc.open(NOMBRE_HOJA_GOOGLE)
    print(f"Hoja de cálculo '{NOMBRE_HOJA_GOOGLE}' abierta.")
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Hoja de cálculo '{NOMBRE_HOJA_GOOGLE}' no encontrada. Creándola...")
    sh = gc.create(NOMBRE_HOJA_GOOGLE)
    # Comparte con tu email si la crea la cuenta de servicio para que la veas
    # Reemplaza 'tu-email@ejemplo.com' con tu email real
    sh.share('tu-email@ejemplo.com', perm_type='user', role='writer')
    # También es buena práctica compartir con la propia cuenta de servicio si la estás creando
    # aunque ya debería tener permisos por ser la creadora.
    # sh.share(creds.service_account_email, perm_type='user', role='writer')
    print(f"Hoja de cálculo '{NOMBRE_HOJA_GOOGLE}' creada y compartida.")


try:
    worksheet = sh.worksheet(NOMBRE_PESTANA_DEV)
    print(f"Pestaña '{NOMBRE_PESTANA_DEV}' encontrada.")
except gspread.exceptions.WorksheetNotFound:
    print(f"Pestaña '{NOMBRE_PESTANA_DEV}' no encontrada. Creándola...")
    worksheet = sh.add_worksheet(title=NOMBRE_PESTANA_DEV)
    print(f"Pestaña '{NOMBRE_PESTANA_DEV}' creada.")

try:
    worksheet_errors = sh.worksheet(NOMBRE_PESTANA_ERRORES)
    print(f"Pestaña '{NOMBRE_PESTANA_ERRORES}' encontrada.")
except gspread.exceptions.WorksheetNotFound:
    print(f"Pestaña '{NOMBRE_PESTANA_ERRORES}' no encontrada. Creándola...")
    worksheet_errors = sh.add_worksheet(title=NOMBRE_PESTANA_ERRORES)
    print(f"Pestaña '{NOMBRE_PESTANA_ERRORES}' creada.")

# --- 3. Leer los datos del archivo CSV usando Pandas ---
try:
    df = pd.read_csv(ARCHIVO_CSV, encoding='utf-8')
    print(f"Datos leídos de '{ARCHIVO_CSV}'. Filas: {len(df)}, Columnas: {len(df.columns)}")

    # Reemplazar NaN con strings vacíos para evitar problemas en Google Sheets
    df_para_subir = df.fillna('')

    # Convertir el DataFrame a una lista de listas (encabezados + datos)
    encabezados = df_para_subir.columns.values.tolist()
    valores = df_para_subir.values.tolist()
    datos_completos_para_subir = [encabezados] + valores

except FileNotFoundError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV}' no fue encontrado.")
    exit()
except pd.errors.EmptyDataError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV}' está vacío.")
    datos_completos_para_subir = []
except Exception as e:
    print(f"Error leyendo el archivo CSV con Pandas: {e}")
    exit()

try:
    df = pd.read_csv(ARCHIVO_CSV_ERRORES, encoding='utf-8')
    print(f"Datos leídos de '{ARCHIVO_CSV_ERRORES}'. Filas: {len(df)}, Columnas: {len(df.columns)}")

    # Reemplazar NaN con strings vacíos para evitar problemas en Google Sheets
    df_para_subir = df.fillna('')

    # Convertir el DataFrame a una lista de listas (encabezados + datos)
    encabezados = df_para_subir.columns.values.tolist()
    valores = df_para_subir.values.tolist()
    datos_completos_para_subir_errores = [encabezados] + valores

except FileNotFoundError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV_ERRORES}' no fue encontrado.")
    exit()
except pd.errors.EmptyDataError:
    print(f"Error: El archivo CSV '{ARCHIVO_CSV_ERRORES}' está vacío.")
    datos_completos_para_subir_errores = []
except Exception as e:
    print(f"Error leyendo el archivo CSV con Pandas: {e}")
    exit()

# --- 4. Subir los datos a la Hoja de Google ---
if datos_completos_para_subir:
    print("Borrando datos existentes en la pestaña...")
    worksheet.clear()

    print(f"Subiendo {len(datos_completos_para_subir)-1} filas de datos (más encabezados) a '{NOMBRE_HOJA_GOOGLE}' -> '{NOMBRE_PESTANA_DEV}'...")
    # worksheet.update('A1', datos_completos_para_subir)
    worksheet.update(datos_completos_para_subir, 'A1') # Forma más moderna
    print("¡Datos subidos exitosamente!")
else:
    print("No hay datos para subir (el CSV podría estar vacío o hubo un error al leerlo).")

if datos_completos_para_subir_errores:
    print("Borrando datos existentes en la pestaña...")
    worksheet_errors.clear()

    print(f"Subiendo {len(datos_completos_para_subir_errores)-1} filas de datos (más encabezados) a '{NOMBRE_HOJA_GOOGLE}' -> '{NOMBRE_PESTANA_ERRORES}'...")
    # worksheet_errors.update('A1', datos_completos_para_subir)
    worksheet_errors.update(datos_completos_para_subir_errores, 'A1') # Forma más moderna
    print("¡Datos subidos exitosamente!")
else:
    print("No hay datos para subir (el CSV podría estar vacío o hubo un error al leerlo).")


print(f"\nPuedes ver la hoja aquí: {sh.url}")