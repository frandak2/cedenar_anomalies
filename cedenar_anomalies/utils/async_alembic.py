# cedenar_anomalies/utils/async_alembic.py
import asyncio
import logging
from typing import List

# Configurar logging
logger = logging.getLogger(__name__)


async def run_alembic_async(args: List[str]) -> bool:
    """Ejecuta comandos de Alembic de forma asíncrona."""
    try:
        # Construir el comando
        cmd = ["alembic"] + args
        logger.info(f"Ejecutando comando: {' '.join(cmd)}")

        # Ejecutar el comando como un proceso separado
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        # Capturar salida y errores
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Comando ejecutado con éxito:\n{stdout.decode()}")
            return True
        else:
            logger.error(f"Error al ejecutar comando:\n{stderr.decode()}")
            return False

    except Exception as e:
        logger.error(f"Error al ejecutar Alembic: {e}")
        return False


async def generate_migration(message: str) -> bool:
    """Genera una nueva migración automática."""
    return await run_alembic_async(["revision", "--autogenerate", "-m", message])


async def upgrade_to_head() -> bool:
    """Actualiza la base de datos a la última revisión."""
    return await run_alembic_async(["upgrade", "head"])


async def downgrade_one_revision() -> bool:
    """Baja una revisión."""
    return await run_alembic_async(["downgrade", "-1"])


async def upgrade_to_revision(revision: str) -> bool:
    """Actualiza la base de datos a una revisión específica."""
    return await run_alembic_async(["upgrade", revision])


async def downgrade_to_revision(revision: str) -> bool:
    """Baja la base de datos a una revisión específica."""
    return await run_alembic_async(["downgrade", revision])


async def get_current_revision() -> str:
    """Obtiene la revisión actual de la base de datos."""
    cmd = ["alembic", "current"]

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, _ = await process.communicate()
    output = stdout.decode().strip()

    if "Current revision" in output and ":" in output:
        revision = output.split(":")[1].strip().split()[0]
        return revision

    return "base"  # Si no hay revisión
