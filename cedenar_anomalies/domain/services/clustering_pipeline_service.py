# cedenar_anomalies/domain/services/clustering_pipeline_service.py

import logging
from typing import Optional

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler, FunctionTransformer
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score, roc_auc_score

from lightgbm import LGBMClassifier

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
        self.numerical_cols = ["LATI_USU", "LONG_USU"]
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

class PipelinePuntaje:
    def __init__(
            self,
            params = {},
            model_dir=models_dir(),
            scores_path="class_puntaje.csv",
            logger: Optional[logging.Logger] = None,
    ):

        self.model_dir = model_dir
        self.params = params
        self.scores_path = models_dir(f"metrics_{scores_path}")
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.scores = []
        self.numerical_cols = ['LATI_USU', 'LONG_USU', 'KVA']
        self.categorical_cols = ['AREA', 'PLAN_COMERCIAL', 'TRAFO_OPEN', 'FASES', 'Zona', 'Codigo']
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Inicializando PipelinePuntaje")

    @staticmethod
    def do_nothing(X):
        return X

    @staticmethod
    def convert_to_categorical(X):
        X_copy = X.copy()
        for col in X_copy.columns:
            X_copy[col] = X_copy[col].astype('category')
        return X_copy

    def build_pipeline(self):
        self.logger.info("Construyendo pipeline de preprocesamiento para puntaje.")

        lgbm_class = LGBMClassifier(verbose=-1, objective='multiclass', **self.params)

        do_nothing_transformer = FunctionTransformer(self.do_nothing)
        convert_type_transformer = FunctionTransformer(self.convert_to_categorical)

        preprocessor = ColumnTransformer(transformers=[
            ('numeric', do_nothing_transformer, self.numerical_cols),
            ('cat', convert_type_transformer, self.categorical_cols)],
            verbose_feature_names_out=False
        )
        preprocessor.set_output(transform='pandas')

        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', lgbm_class)
        ])

        return pipeline

    def fit(self, df: pd.DataFrame) -> Pipeline:
        self.logger.info("Iniciando entrenamiento para puntaje")
        df = df.dropna(subset=['puntaje'])

        # Crea la columna combinada para la estratificación
        # Convertir a string para asegurar que la concatenación funcione bien
        df['puntaje_zona_stratify'] = df['puntaje'].astype(str) + "_" + df['Zona'].astype(str)
        # Verificar la distribución de esta nueva columna
        self.logger.info("Distribución de la columna de estratificación combinada:")
        self.logger.info(df['puntaje_zona_stratify'].value_counts(normalize=True) * 100)

        # Definir tus features (X) y target (y) desde el DataFrame filtrado df_knn_stratify
        X = df
        y = df['puntaje'].astype(int) # El target para KNN
        stratify_col = df['puntaje_zona_stratify']

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.20,       # 80% entrenamiento, 20% prueba
            random_state=42,      # Para reproducibilidad
            stratify=stratify_col # ¡Aquí se usa la columna combinada!
        )

        self.logger.info(f"Tamaño del conjunto de entrenamiento (X_train): {X_train.shape}")
        self.logger.info(f"Tamaño del conjunto de prueba (X_test): {X_test.shape}")

        # Puedes verificar la estratificación (opcional pero recomendado)
        self.logger.info("Distribución del target 'puntaje' en el conjunto de entrenamiento:")
        self.logger.info(y_train.value_counts(normalize=True) * 100)
        self.logger.info("Distribución del target 'puntaje' en el conjunto de prueba:")
        self.logger.info(y_test.value_counts(normalize=True) * 100)
        pipeline = self.build_pipeline()
        le = LabelEncoder()
        y_train_encoded = le.fit_transform(y_train)
        y_test_encoded = le.transform(y_test)
        pipeline.fit(X_train, y_train_encoded)
        y_pred = pipeline.predict(X_test) # Esto devolverá etiquetas codificadas 0..N-1

        # Para las métricas, usa y_test_encoded
        fscore = f1_score(y_test_encoded, y_pred, average='weighted')
        pres = precision_score(y_test_encoded, y_pred, average='weighted', zero_division=0) # Añadido zero_division
        rcall = recall_score(y_test_encoded, y_pred, average='weighted', zero_division=0) # Añadido zero_division
        accu = accuracy_score(y_test_encoded, y_pred)

        # --- CAMBIO CLAVE AQUÍ ---
        y_pred_proba_all_classes = pipeline.predict_proba(X_test) # Obtener probabilidades para TODAS las clases

        # Calcular ROC AUC para multiclase
        # Necesitas y_test original (o y_test_encoded binarizado) y todas las probabilidades
        # 'ovo' (One-vs-One) o 'ovr' (One-vs-Rest)
        # 'average' puede ser 'macro' o 'weighted'
        roc_auc = roc_auc_score(
            y_test_encoded, # Usar las etiquetas codificadas 0..N-1
            y_pred_proba_all_classes,
            multi_class='ovr', # o 'ovo'
            average='weighted' # o 'macro'
        )
        # El DataFrame de scores_data tenía un error de concatenación, lo corrijo
        scores_data = pd.DataFrame({
            'Model': [str(self.params)], # Convertir el objeto modelo a string para el DataFrame
            'F1_Score': [fscore],
            'Precision': [pres],
            'Recall': [rcall],
            'Accuracy': [accu],
            'Roc_auc': [roc_auc]
        })

        scores_data.to_csv(self.scores_path, index=False)

        self.logger.info("Entrenamiento completado")
        joblib.dump(pipeline, self.model_dir / f"pipe_puntaje.pkl")
        self.logger.info(f"Pipeline guardado para puntaje")
        return pipeline

    def predict(self, pipeline: Pipeline, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Iniciando predicción con pipeline entrenado.")
        df = df.copy()

        # Obtener cluster suave
        matrix_pertenencia = pipeline.predict_proba(df)
        puntaje_columns = [f"puntaje_{i+1}" for i in range(matrix_pertenencia.shape[1])]
        matrix_pertenencia_df = pd.DataFrame(matrix_pertenencia, columns=puntaje_columns)

        # Unir con el dataframe original
        df = pd.concat(
            [df.reset_index(drop=True), matrix_pertenencia_df.reset_index(drop=True)],
            axis=1,
        )
        return df

    def load_pipeline(self) -> Pipeline:
        self.logger.info("Cargando pipeline guardado desde disco.")
        model_file = self.model_dir / "pipe_puntaje.pkl"
        
        if model_file.exists():
            pipeline = joblib.load(model_file)
            self.logger.info(f"Pipeline de puntaje cargado correctamente")
            return pipeline
        else:
            self.logger.error(f"No se encontró el archivo del modelo: {model_file}")
            return None


if __name__ == "__main__":
    main()