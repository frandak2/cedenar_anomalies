import logging
import os
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO", log_file: Optional[Path] = None
) -> logging.Logger:
    """
    Configura el sistema de logging para la aplicación.

    Args:
        log_level: Nivel de logging ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        log_file: Ruta opcional a un archivo de log

    Returns:
        Logger configurado
    """
    # Convertir string de nivel a constante de logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    # Configuración básica para logging en consola
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Crear logger para la aplicación
    logger = logging.getLogger("cedenar_anomalies")

    # Añadir handler para archivo si se proporciona ruta
    if log_file:
        # Asegurar que el directorio existe
        log_dir = log_file.parent
        os.makedirs(log_dir, exist_ok=True)

        # Configurar file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        # Añadir handler al logger
        logger.addHandler(file_handler)

    return logger
