# cedenar_anomalies/domain/services/clustering_pipeline_service.py

import logging
from typing import Optional

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

from cedenar_anomalies.domain.models.sklearn_fcm_wrapper import SklearnFCMWrapper

# Importar utilidades para gestión de rutas
from cedenar_anomalies.utils.paths import models_dir

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.info(SklearnFCMWrapper.__module__)


class PipelineClusterFzz:
    def __init__(
        self,
        n_clusters=3,
        m=2,
        distance="euclidean",
        model_dir=models_dir(),
        scores_path="silhouette_scores.csv",
        logger: Optional[logging.Logger] = None,
    ):
        self.n_clusters = n_clusters
        self.model_dir = model_dir
        self.distance = distance
        self.m = m
        self.scores_path = models_dir(f"{distance}_{scores_path}")
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.scores = []
        self.numerical_cols = ["Usuario", "LATI_USU", "LONG_USU"]
        self.categorical_cols = ["AREA", "PLAN_COMERCIAL"]
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Inicializando PipelineClusterFzz")

    def build_pipeline(self):
        self.logger.info("Construyendo pipeline de preprocesamiento y clustering.")

        # self.numerical_cols = ["kWh Rec", "FASES", "KVA", "LATI_USU", "LONG_USU", "puntaje", "evaluacion"]
        # self.categorical_cols = ["AREA", "PLAN_COMERCIAL", "TRAFO_OPEN"]

        numeric_pipeline = Pipeline(
            [("imputer", SimpleImputer(strategy="mean")), ("scaler", StandardScaler())]
        )

        categorical_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="most_frequent")),
                (
                    "labelenc",
                    OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                ),
            ]
        )

        preprocessor = ColumnTransformer(
            [
                ("num", numeric_pipeline, self.numerical_cols),
                ("cat", categorical_pipeline, self.categorical_cols),
            ]
        )

        pipeline = Pipeline(
            [
                ("preprocess", preprocessor),
                (
                    "cluster",
                    SklearnFCMWrapper(
                        n_clusters=self.n_clusters,
                        m=self.m,
                        random_state=42,
                        distance=self.distance,
                        logger=self.logger,
                    ),
                ),
                # ("cluster", FuzzyCMeans(n_clusters=self.n_clusters, random_state=42))  #m=2.0
            ]
        )

        return pipeline

    def fit(self, df: pd.DataFrame, zona: str) -> Pipeline:
        self.logger.info(f"Iniciando entrenamiento para zona: {zona}")
        df_zone = df[df["Zona"] == zona].copy()
        pipeline = self.build_pipeline()
        pipeline.fit(df_zone)
        self.logger.info(f"Entrenamiento completado para zona: {zona}")
        return pipeline

    def predict(self, pipeline: Pipeline, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Iniciando predicción con pipeline entrenado.")
        df = df.copy()

        # Transformar los datos con el preprocesador
        X_transformed = pipeline.named_steps["preprocess"].transform(
            df[self.categorical_cols + self.numerical_cols]
        )

        # Obtener cluster duro
        labels = pipeline.named_steps["cluster"].predict(X_transformed)
        df["cluster_id"] = labels

        # Obtener cluster suave
        matrix_pertenencia = pipeline.named_steps["cluster"].soft_predict(X_transformed)
        cluster_columns = [f"cluster_{i}" for i in range(matrix_pertenencia.shape[1])]
        matrix_pertenencia_df = pd.DataFrame(matrix_pertenencia, columns=cluster_columns)

        # Unir con el dataframe original
        df = pd.concat(
            [df.reset_index(drop=True), matrix_pertenencia_df.reset_index(drop=True)],
            axis=1,
        )
        return df

    def train_by_zone(self, df: pd.DataFrame) -> dict:
        pipelines = {}
        silhouette_scores = []

        self.logger.info("Entrenando pipelines por zona.")
        for zona in df["Zona"].unique():
            self.logger.info(f"Procesando zona: {zona}")
            df_zone = df[df["Zona"] == zona].copy()
            pipeline = self.fit(df, zona)
            df_with_clusters = self.predict(pipeline, df_zone)

            X_transformed = pipeline.named_steps["preprocess"].transform(
                df_zone[self.categorical_cols + self.numerical_cols]
            )
            score = silhouette_score(X_transformed, df_with_clusters["cluster_id"])
            self.logger.info(f"Índice de silueta para zona {zona}: {score:.4f}")

            pc = pipeline.named_steps["cluster"].pc_
            pec = pipeline.named_steps["cluster"].pec_

            joblib.dump(pipeline, self.model_dir / f"pipeline_{zona}.pkl")
            self.logger.info(f"Pipeline guardado para zona {zona}")

            silhouette_scores.append(
                {"zona": zona, "silhouette_score": score, "PC": pc, "PEC": pec}
            )
            pipelines[zona] = pipeline

        pd.DataFrame(silhouette_scores).to_csv(self.scores_path, index=False)
        self.logger.info(f"Scores de silueta guardados en: {self.scores_path}")
        return pipelines

    def predict_all_zones(self, df: pd.DataFrame, pipelines: dict) -> pd.DataFrame:
        self.logger.info("Iniciando predicción en todas las zonas.")
        df_all = []
        for zona, pipeline in pipelines.items():
            self.logger.info(f"Prediciendo zona: {zona}")
            df_zone = df[df["Zona"] == zona].copy()
            df_clustered = self.predict(pipeline, df_zone)
            df_all.append(df_clustered)
        return pd.concat(df_all, ignore_index=True)

    def load_pipelines(self) -> dict:
        self.logger.info("Cargando pipelines guardados desde disco.")
        pipelines = {}
        for model_file in self.model_dir.glob("pipeline_*.pkl"):
            zona = model_file.stem.replace("pipeline_", "")
            pipelines[zona] = joblib.load(model_file)
            self.logger.info(f"Pipeline cargado para zona: {zona}")
        return pipelines


if __name__ == "__main__":
    main()
