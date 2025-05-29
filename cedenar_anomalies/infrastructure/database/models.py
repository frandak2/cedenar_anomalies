# cedenar_anomalies/infrastructure/database/models.py
from sqlalchemy import Boolean, Column, Float, Integer, String, Text

from cedenar_anomalies.infrastructure.database.session import Base


class AnomaliaData(Base):
    __tablename__ = "anomalia_data"

    id = Column(String, primary_key=True)

    # Otros campos
    AREA = Column(String(255), nullable=True)
    item_288 = Column(Integer, nullable=False)
    odt = Column(Integer, nullable=False)
    orden = Column(Integer, nullable=False)
    PLAN_COMERCIAL = Column(String(255), nullable=False)
    Descripcion = Column(Text, nullable=True)
    reincidente = Column(String(50), nullable=False, default="NO")
    Anomalia_conf = Column(String(255), nullable=False, default="Anomalia")
    ZONA = Column(String(100), nullable=False)
    año = Column(Integer, nullable=False)  # Año de la anomalía
    # Campos float
    LATI_USU = Column(Float, nullable=False)
    LONG_USU = Column(Float, nullable=False)
    NIVEL = Column(Float, nullable=False, default=0.0)

    # Campos booleanos
    item_68 = Column(Boolean, nullable=False, default=False)
    item_74 = Column(Boolean, nullable=False, default=False)
    item_237 = Column(Boolean, nullable=False, default=False)

    # Campos string/object
    item_248 = Column(String(255), nullable=False, default="NO INDICA")
    item_597 = Column(String(255), nullable=False, default="NOAPL")
    item_602 = Column(String(255), nullable=False, default="NOAPL")
    item_108 = Column(String(255), nullable=False, default="NO INDICA")
    item_43 = Column(String(255), nullable=False, default="NO INDICA")
    item_603 = Column(String(255), nullable=False, default="NOAPL")
    item_599 = Column(String(255), nullable=False, default="NOAPL")
    item_35 = Column(String(255), nullable=False, default="NO INDICA")
    item_598 = Column(String(255), nullable=False, default="NOAPL")
    item_33 = Column(String(255), nullable=False, default="NO INDICA")
    item_601 = Column(String(255), nullable=False, default="NOAPL")
    item_24 = Column(String(255), nullable=False, default="NO")  # SI/NO
    item_23 = Column(String(255), nullable=False, default="NO")  # SI/NO

    # Metadata
    fecha_creacion = Column(String(50), nullable=True)  # Fecha de creación del registro

    # Para búsqueda por texto
    # tags = Column(MutableList.as_mutable(ARRAY(String)), nullable=True)

    def __repr__(self):
        return (
            f"<AnomaliaData(id={self.id},",
            f" odt={self.odt},",
            f" Anomalia_conf={self.Anomalia_conf})>",
        )


class InferenceModel(Base):
    __tablename__ = "inference_model"

    # Definir un ID autoincremental como clave primaria
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Campos basados en el DataFrame proporcionado
    Orden = Column(Integer, nullable=False)
    Usuario = Column(Integer, nullable=False)
    Ejecucion = Column(String(255), nullable=False)
    Codigo = Column(String(255), nullable=True)
    Descripcion = Column(Text, nullable=False)
    Motivo = Column(Text, nullable=True)
    kWh_Rec = Column(
        Float, nullable=True
    )  # Se cambió 'kWh Rec' a 'kWh_Rec' para evitar espacios
    Factor = Column(Float, nullable=True)
    data_id = Column(Float, nullable=True)  # Renombrado desde 'id' para evitar conflicto
    Nombre = Column(String(255), nullable=True)
    Factor_1 = Column(
        Float, nullable=True
    )  # Se cambió 'Factor.1' a 'Factor_1' para evitar puntos
    AREA = Column(String(255), nullable=True)
    PLAN_COMERCIAL = Column(String(255), nullable=True)
    TRAFO_OPEN = Column(String(255), nullable=True)
    FASES = Column(Float, nullable=True)
    KVA = Column(Float, nullable=True)
    LATI_USU = Column(Float, nullable=True)
    LONG_USU = Column(Float, nullable=True)
    puntaje = Column(Float, nullable=True)
    evaluacion = Column(Float, nullable=True)
    Zona = Column(String(255), nullable=False)
    cluster_id = Column(Integer, nullable=False)
    cluster_0 = Column(Float, nullable=False)
    cluster_1 = Column(Float, nullable=False)
    cluster_2 = Column(Float, nullable=False)

    # Metadata
    fecha_carga = Column(String(50), nullable=True)  # Fecha de carga del registro

    def __repr__(self):
        return (
            f"<InferenceModel(id={self.id}, Orden={self.Orden}, Usuario={self.Usuario})>"
        )
