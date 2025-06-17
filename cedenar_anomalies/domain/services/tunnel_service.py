import logging
from typing import Any, Dict, Optional


class TunnelService:
    """Servicio de dominio para gestión de túneles de conexión externa"""

    def __init__(self, tunnel_adapter, logger: Optional[logging.Logger] = None):
        self.tunnel_adapter = tunnel_adapter
        self.logger = logger or logging.getLogger(__name__)
        self.tunnel_info = None

    def start_tunnel(self) -> Dict[str, Any]:
        """Inicia un túnel para exponer un servicio local"""
        self.logger.info("Iniciando túnel para exposición de servicio")
        self.tunnel_info = self.tunnel_adapter.start_tunnel()
        return self.tunnel_info

    def stop_tunnel(self) -> None:
        """Detiene el túnel activo"""
        if self.tunnel_info:
            self.logger.info("Deteniendo túnel activo")
            self.tunnel_adapter.stop_tunnel()
            self.tunnel_info = None

    def get_tunnel_info(self) -> Optional[Dict[str, Any]]:
        """Obtiene información del túnel activo"""
        return self.tunnel_info
