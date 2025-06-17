from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd

from cedenar_anomalies.infrastructure.database.models import AnomaliaData


class IAnomaliaRepository(ABC):
    """Puerto (interfaz) para el repositorio de anomalías"""

    @abstractmethod
    def save(self, anomalia: AnomaliaData) -> AnomaliaData:
        """Guarda una anomalía en el repositorio"""
        pass

    @abstractmethod
    def save_batch(self, anomalias: List[AnomaliaData]) -> int:
        """Guarda un lote de anomalías y retorna el número guardado"""
        pass

    @abstractmethod
    def find_by_id(self, id: int) -> Optional[AnomaliaData]:
        """Busca una anomalía por su ID"""
        pass

    @abstractmethod
    def find_all(self) -> List[AnomaliaData]:
        """Retorna todas las anomalías"""
        pass

    @abstractmethod
    def find_by_criteria(self, criteria: Dict[str, Any]) -> List[AnomaliaData]:
        """Busca anomalías por criterios específicos"""
        pass

    @abstractmethod
    def save_from_dataframe(self, df: pd.DataFrame) -> int:
        """Guarda anomalías desde un DataFrame"""
        pass

    @abstractmethod
    def find_by_year(self, year: int) -> List[AnomaliaData]:
        """Busca anomalías por año"""
        pass


class IApiClient(ABC):
    """Puerto (interfaz) para cliente de API externa"""

    @abstractmethod
    async def fetch_anomalias(self) -> Dict[str, Any]:
        """Obtiene datos de anomalías desde la API externa"""
        pass

    @abstractmethod
    async def process_api_data(self, data: Dict[str, Any]) -> List[AnomaliaData]:
        """Procesa los datos de la API y los convierte a entidades del dominio"""
        pass
