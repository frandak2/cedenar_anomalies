# run_ngrok_for_looker.py (en la raíz del proyecto)

import os
import logging
import time
from dotenv import load_dotenv

# Importar las funciones de tu ngrok_manager
from cedenar_anomalies.infrastructure.adapters.ngrok_connector.ngrok_manager import (
    start_ngrok_tunnel,
    stop_ngrok_tunnel,
    configure_ngrok_auth_token
)

# (Opcional pero recomendado) Importar y configurar tu logger del proyecto
# Asumiremos que tienes una función para configurar el logging en utils/logging_config.py
try:
    from cedenar_anomalies.utils.logging_config import setup_logging
    setup_logging() # Configura el logging según tu proyecto
except ImportError:
    # Fallback a configuración básica si no existe o falla la importación
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logging.warning("No se pudo cargar la configuración de logging del proyecto. Usando configuración básica.")

logger = logging.getLogger(__name__)

def main():
    # Cargar variables de entorno desde .env
    load_dotenv()

    ngrok_auth_token = os.getenv("NGROK_AUTH_TOKEN")
    if not ngrok_auth_token:
        logger.error("Error: NGROK_AUTH_TOKEN no está configurado en el archivo .env.")
        logger.error("Por favor, añade NGROK_AUTH_TOKEN='tu_token' a tu archivo .env")
        return

    # Configurar el token de Ngrok (esto ya lo hace start_ngrok_tunnel, pero es bueno ser explícito)
    if not configure_ngrok_auth_token(ngrok_auth_token):
        logger.error("Fallo al configurar el token de autenticación de Ngrok.")
        return

    # Obtener el puerto de la base de datos.
    # Idealmente, esto vendría de tu cedenar_anomalies.utils.config
    # Por ahora, podemos tomarlo de una variable de entorno o hardcodearlo como ejemplo.
    try:
        # Intenta leer desde variable de entorno
        db_port_str = os.getenv("DB_PORT", "5432") # Usar DB_PORT o valor por defecto 5432
        db_port = int(db_port_str)
    except ValueError:
        logger.error(f"Valor inválido para POSTGRES_PORT: '{db_port_str}'. Usando el puerto por defecto 5432.")
        db_port = 5432

    logger.info(f"Intentando exponer la base de datos local en el puerto {db_port} a través de Ngrok...")

    public_url = None
    try:
        public_url = start_ngrok_tunnel(port=db_port, proto="tcp", name="looker_studio_db_tunnel")
        if public_url:
            host = public_url.split('://')[1].split(':')[0]
            port = public_url.split('://')[1].split(':')[1]

            logger.info("--------------------------------------------------------------------")
            logger.info("¡Túnel Ngrok activo! Conecta tu base de datos desde Looker Studio:")
            logger.info(f"  Host/Servidor: {host}")
            logger.info(f"  Puerto: {port}")
            logger.info(f"  Base de datos: (el nombre de tu base de datos local)")
            logger.info(f"  Usuario: (tu usuario de PostgreSQL)")
            logger.info(f"  Contraseña: (tu contraseña de PostgreSQL)")
            logger.info("--------------------------------------------------------------------")
            logger.info("El túnel permanecerá activo. Presiona Ctrl+C para detenerlo.")

            # Mantener el script corriendo
            while True:
                time.sleep(1) # Evita que el bucle consuma CPU innecesariamente

        else:
            logger.error("No se pudo iniciar el túnel Ngrok. Revisa los logs anteriores para más detalles.")

    except KeyboardInterrupt:
        logger.info("\nCierre solicitado por el usuario (Ctrl+C).")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
    finally:
        logger.info("Deteniendo el túnel Ngrok...")
        stop_ngrok_tunnel()
        logger.info("Túnel Ngrok detenido. El script ha finalizado.")

if __name__ == "__main__":
    main()