# cedenar_anomalies/infrastructure/adapters/ngrok_connector/ngrok_manager.py

from pyngrok import ngrok, conf
import logging
import os
# Cargar .env para la prueba (si ejecutas este script directamente)
from dotenv import load_dotenv
import time
# Cargar variables de entorno desde el archivo .env
load_dotenv()

logger = logging.getLogger(__name__)

# Variable global para mantener el túnel si se necesita acceder a él después
_current_tunnel = None

def configure_ngrok_auth_token(auth_token: str | None = None):
    """
    Configura el token de autenticación de Ngrok.
    Intenta obtenerlo de la variable de entorno NGROK_AUTH_TOKEN si no se proporciona.
    """
    if auth_token is None:
        auth_token = os.getenv("NGROK_AUTH_TOKEN")

    if not auth_token:
        logger.warning("NGROK_AUTH_TOKEN no está configurado. Ngrok podría tener funcionalidades limitadas o no funcionar.")
        return False # O levantar una excepción si es crítico

    try:
        ngrok.set_auth_token(auth_token)
        logger.info("Token de autenticación de Ngrok configurado correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error al configurar el token de Ngrok: {e}")
        return False


def start_ngrok_tunnel(port: int, proto: str = "tcp", name: str | None = None) -> str | None:
    """
    Inicia un túnel Ngrok al puerto y protocolo especificados.

    Args:
        port (int): El puerto local al que apuntará el túnel.
        proto (str): El protocolo a usar ('tcp', 'http', 'tls'). Por defecto 'tcp' para bases de datos.
        name (str, optional): Un nombre para el túnel (útil si tienes múltiples túneles).

    Returns:
        str | None: La URL pública del túnel si tiene éxito, None en caso contrario.
    """
    global _current_tunnel
    if not configure_ngrok_auth_token(): # Asegurarse de que el token está configurado
        logger.error("No se pudo configurar el token de Ngrok. No se iniciará el túnel.")
        return None

    try:
        # Detener cualquier túnel existente gestionado por este manager
        stop_ngrok_tunnel()

        logger.info(f"Iniciando túnel Ngrok {proto} en el puerto local {port}...")
        tunnel_options = {"proto": proto, "addr": port}
        if name:
            tunnel_options["name"] = name

        _current_tunnel = ngrok.connect(**tunnel_options)
        public_url = _current_tunnel.public_url
        logger.info(f"Túnel Ngrok activo: {public_url} -> localhost:{port}")
        return public_url
    except Exception as e:
        logger.error(f"Error al iniciar el túnel Ngrok: {e}")
        _current_tunnel = None # Asegurarse de que está limpio si falla
        return None


def stop_ngrok_tunnel():
    """
    Detiene todos los túneles Ngrok activos o un túnel específico si se gestiona.
    """
    global _current_tunnel
    if _current_tunnel:
        logger.info(f"Deteniendo túnel Ngrok: {_current_tunnel.public_url}")
        ngrok.disconnect(_current_tunnel.public_url)
        ngrok.kill() # Asegura que el proceso ngrok se cierre completamente
        _current_tunnel = None
        logger.info("Túnel Ngrok detenido.")
    else:
        # Si no hay un túnel específico gestionado, intentar cerrar todos por si acaso
        active_tunnels = ngrok.get_tunnels()
        if active_tunnels:
            logger.info("Deteniendo todos los túneles Ngrok activos...")
            for tunnel in active_tunnels:
                ngrok.disconnect(tunnel.public_url)
            ngrok.kill()
            logger.info("Todos los túneles Ngrok activos han sido detenidos.")
        else:
            logger.info("No hay túneles Ngrok activos para detener.")

def get_active_tunnel_url() -> str | None:
    """
    Devuelve la URL pública del túnel activo gestionado por este manager.
    """
    if _current_tunnel:
        return _current_tunnel.public_url
    return None

# Ejemplo de cómo se podría usar (para pruebas, no para producción directa aquí)
if __name__ == "__main__":
    # Asumiendo que .env está en la raíz del proyecto, dos niveles arriba de este script
    # dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env')
    # load_dotenv()

    logging.basicConfig(level=logging.INFO)

    # Puerto de tu PostgreSQL local
    DB_PORT = 5432 # O tómalo de una variable de entorno/configuración

    public_url = start_ngrok_tunnel(DB_PORT)

    if public_url:
        print(f"Ngrok está corriendo. Puedes conectar a tu DB usando: {public_url}")
        print("Host:", public_url.split('://')[1].split(':')[0])
        print("Port:", public_url.split('://')[1].split(':')[1])
        print("Presiona Ctrl+C para detener.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Cerrando túnel...")
    else:
        print("No se pudo iniciar el túnel Ngrok.")

    stop_ngrok_tunnel()
    print("Script finalizado.")