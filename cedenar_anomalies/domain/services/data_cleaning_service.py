import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Esta es una clase de dominio, así que no debemos tener dependencias directas
# con la infraestructura, usamos parámetros en su lugar


class DataCleaningService:
    """
    Servicio de dominio para limpiar y transformar datos.

    Este servicio toma un DataFrame y realiza operaciones de limpieza como
    tratamiento de valores nulos, eliminación de duplicados, y
    transformaciones de datos para prepararlo para análisis posterior.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        output_filename: Optional[str] = None,
        columns_to_keep: Optional[List[str]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Inicializa el servicio con el DataFrame a limpiar.

        Args:
            df: DataFrame de entrada que requiere limpieza
            output_filename: Nombre opcional del archivo donde se guardará el resultado
            columns_to_keep: Lista opcional de columnas a conservar en el resultado final
            logger: Logger opcional para registrar las operaciones
        """
        self.df = df.copy()
        self.output_filename = output_filename or "cleaned_data.csv"
        self.original_shape = self.df.shape
        self.report = {}

        # Si no se especifican columnas, usar todas las disponibles
        self.columns_to_keep = columns_to_keep or self.df.columns.tolist()

        # Configuración del logging
        self.logger = logger or logging.getLogger(__name__)

        # Asegurar que 'año' esté en las columnas a conservar si existe en el DataFrame
        if "año" in self.df.columns and "año" not in self.columns_to_keep:
            self.columns_to_keep.append("año")

    def generate_initial_report(self) -> Dict:
        """
        Genera un informe del estado inicial del DataFrame antes de la limpieza.

        Returns:
            Diccionario con estadísticas del DataFrame
        """
        self.logger.info("Generando reporte inicial del DataFrame...")

        report = {
            "original_shape": self.original_shape,
            "missing_values": self.df.isna().sum().to_dict(),
            "duplicates": {
                "orden_odt": self.df.duplicated(subset=["orden", "odt"], keep=False).sum()
            },
            "dtypes": {col: str(dtype) for col, dtype in self.df.dtypes.items()},
        }

        self.logger.info(
            f"DataFrame original: {self.original_shape[0]} filas,"
            f" {self.original_shape[1]} columnas"
        )
        self.report["initial"] = report
        return report

    def _remove_duplicates(self, subset: List[str] = ["orden", "odt"]) -> pd.DataFrame:
        """
        Elimina registros duplicados en las columnas especificadas.

        Args:
            subset: Lista de columnas para verificar duplicados

        Returns:
            DataFrame sin duplicados
        """
        self.logger.info(f"Removiendo duplicados basados en columnas: {subset}")

        duplicates_count = self.df.duplicated(subset=subset, keep=False).sum()
        self.logger.info(f"Se encontraron {duplicates_count} registros duplicados")

        # Guardar la información sobre duplicados en el reporte
        self.report["duplicates_removed"] = duplicates_count

        # Eliminar duplicados conservando la primera aparición
        self.df = self.df.drop_duplicates(subset=subset, keep="first")

        self.logger.info(f"Dataframe después de eliminar duplicados: {self.df.shape}")
        return self.df

    def _remove_null_coordinates(
        self, coordinate_columns: List[str] = ["LATI_USU"]
    ) -> pd.DataFrame:
        """
        Elimina registros que tienen valores nulos
         en las columnas de coordenadas especificadas.

        Args:
            coordinate_columns: Lista de columnas de coordenadas a verificar

        Returns:
            DataFrame sin registros con coordenadas nulas
        """
        self.logger.info(
            f"Eliminando registros con coordenadas nulas en: {coordinate_columns}"
        )

        # Guardar la cantidad original de registros
        original_count = len(self.df)

        # Eliminar registros con valores nulos en las columnas especificadas
        self.df = self.df.dropna(subset=coordinate_columns)

        # Calcular cuántos registros se eliminaron
        removed_count = original_count - len(self.df)
        removed_percentage = (
            round((removed_count / original_count) * 100, 2) if original_count > 0 else 0
        )

        self.logger.info(
            f"Se eliminaron {removed_count} registros"
            f" con coordenadas nulas ({removed_percentage}%)"
        )

        # Guardar la información en el reporte
        self.report["null_coordinates_removed"] = {
            "columns": coordinate_columns,
            "count": removed_count,
            "percentage": removed_percentage,
        }

        return self.df

    def _handle_boolean_columns(self, boolean_columns: List[str]) -> pd.DataFrame:
        """
        Convierte los valores NaN a False en columnas booleanas.

        Args:
            boolean_columns: Lista de columnas a tratar como booleanas

        Returns:
            DataFrame con las columnas booleanas tratadas
        """
        self.logger.info(f"Procesando columnas booleanas: {boolean_columns}")

        for column in boolean_columns:
            if column in self.df.columns:
                nan_count = self.df[column].isna().sum()
                if nan_count > 0:
                    self.logger.info(
                        f"Reemplazando {nan_count} valores NaN con False en {column}"
                    )
                    self.df[column] = self.df[column].replace(np.nan, False)

                # Convertir 'TRUE'/'FALSE' a True/False si existen
                if self.df[column].dtype == "object":
                    self.df[column] = self.df[column].replace(
                        {"TRUE": True, "FALSE": False}
                    )

        return self.df

    def _handle_categorical_columns(
        self, categorical_columns: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Reemplaza valores NaN en columnas categóricas con un valor predeterminado.

        Args:
            categorical_columns: Diccionario de columnas con sus valores por defecto

        Returns:
            DataFrame con las columnas categóricas tratadas
        """
        self.logger.info("Procesando columnas categóricas")

        for column, default_value in categorical_columns.items():
            if column in self.df.columns:
                nan_count = self.df[column].isna().sum()
                if nan_count > 0:
                    self.logger.info(
                        f"Reemplazando {nan_count} valores NaN"
                        f" con '{default_value}' en {column}"
                    )
                    self.df[column] = self.df[column].replace(np.nan, default_value)

        return self.df

    def _handle_binary_indicators(
        self, binary_columns: Dict[str, Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Convierte columnas a indicadores binarios (SI/NO).

        Args:
            binary_columns: Diccionario con columnas y valores a reemplazar

        Returns:
            DataFrame con valores convertidos a indicadores binarios
        """
        self.logger.info("Procesando columnas con indicadores binarios")

        for column, replacements in binary_columns.items():
            if column in self.df.columns:
                for original, replacement in replacements.items():
                    self.df[column] = self.df[column].replace(original, replacement)

                self.logger.info(
                    f"Columna {column} convertida a formato binario:"
                    f" {self.df[column].value_counts().to_dict()}"
                )

        return self.df

    def _mark_recurrences(self, id_column: str = "item_288") -> pd.DataFrame:
        """
        Marca registros que aparecen más de una vez como reincidentes.

        Args:
            id_column: Columna identificadora para verificar reincidencias

        Returns:
            DataFrame con la columna 'reincidente' añadida
        """
        self.logger.info(f"Marcando reincidencias basadas en la columna: {id_column}")

        if id_column in self.df.columns:
            # Identificar registros duplicados
            duplicated = self.df.duplicated(subset=id_column, keep=False)

            # Crear nueva columna 'reincidente'
            self.df["reincidente"] = duplicated.apply(lambda x: "SI" if x else "NO")

            reincidentes_count = (self.df["reincidente"] == "SI").sum()
            self.logger.info(
                f"Se marcaron {reincidentes_count} registros como reincidentes"
            )

            # Guardar estadísticas en el reporte
            self.report["reincidences"] = {
                "column": id_column,
                "count": reincidentes_count,
                "percentage": round((reincidentes_count / len(self.df)) * 100, 2),
            }
        else:
            self.logger.warning(
                f"La columna {id_column} no existe en el DataFrame."
                f" No se pueden marcar reincidencias."
            )

        return self.df

    def _create_anomaly_column(self) -> pd.DataFrame:
        """
        Crea la columna 'Anomalia_conf' basada en el valor de la columna 'Descripcion'.

        Si 'Descripcion' es 'Solo odt', se marca como 'No anomalia',
        de lo contrario se marca como 'Anomalia'.

        Returns:
            DataFrame con la columna 'Anomalia_conf' añadida
        """
        self.logger.info("Creando columna de clasificación de anomalías")

        if "Descripcion" in self.df.columns:

            def determinar_anomalia(descripcion):
                if descripcion == "Solo odt":
                    return "No anomalia"
                else:
                    return "Anomalia"

            # Crear la columna Anomalia_conf
            self.df["Anomalia_conf"] = self.df["Descripcion"].apply(determinar_anomalia)

            # Contar y registrar estadísticas
            anomalia_count = (self.df["Anomalia_conf"] == "Anomalia").sum()
            no_anomalia_count = (self.df["Anomalia_conf"] == "No anomalia").sum()

            self.logger.info(
                f"Se clasificaron {anomalia_count} registros"
                f" como 'Anomalia' y {no_anomalia_count} como 'No anomalia'"
            )

            # Guardar estadísticas en el reporte
            self.report["anomaly_classification"] = {
                "anomalias": anomalia_count,
                "no_anomalias": no_anomalia_count,
                "porcentaje_anomalias": round((anomalia_count / len(self.df)) * 100, 2)
                if len(self.df) > 0
                else 0,
            }
        else:
            self.logger.warning(
                "La columna 'Descripcion' no existe en el DataFrame."
                " No se puede crear la clasificación de anomalías."
            )

        return self.df

    def _select_columns(self) -> pd.DataFrame:
        """
        Selecciona solo las columnas especificadas para el DataFrame final.

        Returns:
            DataFrame con solo las columnas seleccionadas
        """
        self.logger.info(
            f"Seleccionando {len(self.columns_to_keep)} columnas para el DataFrame final"
        )

        # Verificar que todas las columnas existan
        missing_columns = [
            col for col in self.columns_to_keep if col not in self.df.columns
        ]
        if missing_columns:
            self.logger.warning(
                f"Las siguientes columnas no existen en el DataFrame: {missing_columns}"
            )

        # Filtrar solo las columnas existentes
        valid_columns = [col for col in self.columns_to_keep if col in self.df.columns]

        self.df = self.df[valid_columns]
        self.logger.info(f"DataFrame filtrado: {self.df.shape}")

        return self.df

    def generate_final_report(self) -> Dict:
        """
        Genera un informe del estado final del DataFrame después de la limpieza.

        Returns:
            Diccionario con estadísticas del DataFrame limpio
        """
        self.logger.info("Generando reporte final del DataFrame limpio...")

        final_report = {
            "final_shape": self.df.shape,
            "missing_values": self.df.isna().sum().to_dict(),
            "data_types": {col: str(dtype) for col, dtype in self.df.dtypes.items()},
            "categorical_columns": {
                col: self.df[col].value_counts().to_dict()
                for col in self.df.select_dtypes(include=["object", "category"]).columns
            },
        }

        # Cálculo de la reducción
        rows_reduction = self.original_shape[0] - self.df.shape[0]
        cols_reduction = self.original_shape[1] - self.df.shape[1]

        final_report["reduction"] = {
            "rows": rows_reduction,
            "rows_percentage": round((rows_reduction / self.original_shape[0]) * 100, 2)
            if self.original_shape[0] > 0
            else 0,
            "columns": cols_reduction,
            "columns_percentage": round(
                (cols_reduction / self.original_shape[1]) * 100, 2
            )
            if self.original_shape[1] > 0
            else 0,
        }

        self.logger.info(
            f"DataFrame final: {self.df.shape[0]} filas, {self.df.shape[1]} columnas"
        )
        self.logger.info(
            f"Reducción: {rows_reduction} filas "
            f"({final_report['reduction']['rows_percentage']}%), "
            f"{cols_reduction} columnas "
            f"({final_report['reduction']['columns_percentage']}%)"
        )

        self.report["final"] = final_report
        return final_report

    def get_cleaned_data(self) -> pd.DataFrame:
        """
        Devuelve el DataFrame limpio.

        Returns:
            DataFrame limpio
        """
        return self.df

    def get_report(self) -> Dict:
        """
        Devuelve el reporte generado durante la limpieza.

        Returns:
            Diccionario con el reporte completo
        """
        return self.report

    def clean(self) -> pd.DataFrame:
        """
        Ejecuta todo el proceso de limpieza en el DataFrame.

        Returns:
            DataFrame limpio
        """
        self.logger.info("Iniciando proceso de limpieza de datos...")

        # Generar reporte inicial
        self.generate_initial_report()

        # Eliminar duplicados
        self._remove_duplicates()

        # Eliminar registros con coordenadas nulas
        self._remove_null_coordinates(["LATI_USU"])

        # Procesar columnas booleanas - reemplazar NaN con False
        boolean_columns = ["item_237", "item_74", "item_68"]
        self._handle_boolean_columns(boolean_columns)

        # Procesar columnas categóricas - reemplazar NaN con valores predeterminados
        categorical_defaults = {
            "item_601": "NOAPL",
            "item_33": "NO INDICA",
            "item_598": "NOAPL",
            "item_35": "NO INDICA",
            "item_599": "NOAPL",
            "item_603": "NOAPL",
            "item_43": "NO INDICA",
            "item_108": "NO INDICA",
            "item_602": "NOAPL",
            "item_597": "NOAPL",
            "item_248": "NO INDICA",
        }
        self._handle_categorical_columns(categorical_defaults)

        # Procesar columnas con indicadores binarios (SI/NO)
        binary_columns = {
            "item_23": {np.nan: "NO", "TRUE": "SI"},
            "item_24": {np.nan: "NO", "TRUE": "SI"},
        }
        self._handle_binary_indicators(binary_columns)

        # Marcar reincidencias
        self._mark_recurrences()

        # Crear columna de clasificación de anomalías
        self._create_anomaly_column()

        # Seleccionar columnas finales si se especificaron
        if self.columns_to_keep and len(self.columns_to_keep) < len(self.df.columns):
            self._select_columns()

        # Generar reporte final
        self.generate_final_report()

        self.logger.info("Proceso de limpieza completado con éxito")
        return self.df

    def prepare_dataset(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara un conjunto de datos aplicando el proceso de limpieza.

        Args:
            raw_data: DataFrame con los datos crudos a procesar

        Returns:
            DataFrame limpio y procesado
        """
        # Asignar el nuevo DataFrame al servicio
        self.df = raw_data.copy()
        self.original_shape = self.df.shape
        self.report = {}

        # Aplicar el proceso de limpieza
        return self.clean()
