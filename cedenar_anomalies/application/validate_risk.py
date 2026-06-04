# cedenar_anomalies/application/validate_risk.py
"""Valida el modelo de RIESGO por usuario con CV out-of-fold.

Compara el nuevo enfoque (nivel usuario + features de cluster + class_weight)
contra las líneas base. Reporta accuracy, F1 macro/weighted, recall por clase y
ROC-AUC, además del gap train/test (overfitting). NO toca modelos de producción.
"""
import logging

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, LabelEncoder

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelineClusterFzz,
)
from cedenar_anomalies.utils.paths import data_interim_dir

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("validate_risk")
logger.setLevel(logging.INFO)

CURRENT_PARAMS = {
    "n_estimators": 468,
    "learning_rate": 0.027112035074244662,
    "num_leaves": 116,
    "max_depth": 12,
    "min_child_samples": 22,
    "max_bin": 225,
    "reg_alpha": 0.003891437220124611,
    "reg_lambda": 0.8161960202355869,
    "min_gain_to_split": 7.269371017270656,
    "bagging_fraction": 0.9243380690332376,
    "bagging_freq": 3,
    "feature_fraction": 0.9616425348024227,
}

NUM_COLS = ["LATI_USU", "LONG_USU", "LATI_TRAFO", "LONG_TRAFO"]
CAT_COLS = [
    "TRAFO_OPEN",
    "FASES",
    "KVA",
    "AREA",
    "PLAN_COMERCIAL",
    "ZONA",
    "CATEGORIA",
    "SUB_CATEGORIA",
]
CLUSTER_NUM = ["cluster_0", "cluster_1", "cluster_2"]
CLUSTER_CAT = ["cluster_id"]


def to_category(x):
    out = x.copy()
    for col in out.columns:
        out[col] = out[col].astype("category")
    return out


def add_cluster_features(user_df):
    """Clustering FCM por zona (no supervisado) para añadir features de cluster."""
    cl = PipelineClusterFzz(logger=logger)
    parts = []
    for zona in user_df["ZONA"].unique():
        pipe = cl.fit(user_df, zona)  # entrena en memoria (no vuelca pkl)
        dz = user_df[user_df["ZONA"] == zona].copy()
        parts.append(cl.predict(pipe, dz))
    return pd.concat(parts, ignore_index=True)


def build_clf(num_cols, cat_cols, params):
    pre = ColumnTransformer(
        transformers=[
            ("num", FunctionTransformer(lambda v: v), num_cols),
            ("cat", FunctionTransformer(to_category), cat_cols),
        ],
        verbose_feature_names_out=False,
    )
    pre.set_output(transform="pandas")
    clf = LGBMClassifier(
        verbose=-1, objective="multiclass", class_weight="balanced", **params
    )
    return Pipeline(steps=[("pre", pre), ("clf", clf)])


def evaluate(df, num_cols, cat_cols, label):
    y = df["puntaje"].astype(int).to_numpy()
    classes = np.sort(np.unique(y))
    le = LabelEncoder().fit(classes)
    y_enc = le.transform(y)
    n = len(df)
    oof_pred = np.empty(n, dtype=int)
    oof_proba = np.zeros((n, len(classes)))
    train_accs = []
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for tr, te in skf.split(df, y_enc):
        model = build_clf(num_cols, cat_cols, CURRENT_PARAMS)
        model.fit(df.iloc[tr], y_enc[tr])
        train_accs.append(model.score(df.iloc[tr], y_enc[tr]))
        oof_pred[te] = model.predict(df.iloc[te])
        oof_proba[te] = model.predict_proba(df.iloc[te])
    print(f"\n===== {label} =====")
    print(f"accuracy:        {accuracy_score(y_enc, oof_pred):.4f}")
    print(f"F1 macro:        {f1_score(y_enc, oof_pred, average='macro'):.4f}")
    print(f"F1 weighted:     {f1_score(y_enc, oof_pred, average='weighted'):.4f}")
    print(
        f"ROC-AUC ovr:     "
        f"{roc_auc_score(y_enc, oof_proba, multi_class='ovr', average='weighted'):.4f}"
    )
    print(f"train acc media: {np.mean(train_accs):.4f}  (gap overfitting)")
    print("reporte por clase (recall importa en clases 3/4):")
    print(classification_report(y_enc, oof_pred, zero_division=0))


def main():
    user_df = pd.read_csv(data_interim_dir("02_dataset_train_user.csv"))
    n = len(user_df)
    maj = user_df["puntaje"].value_counts().max() / n
    print(f"usuarios: {n} | línea base clase mayoritaria (accuracy): {maj:.4f}")
    print("Referencia modelo actual (por anomalía): accuracy ~0.44, ROC-AUC ~0.53")

    print("\n>>> Añadiendo features de cluster (FCM por zona)...")
    clustered = add_cluster_features(user_df)

    evaluate(user_df, NUM_COLS, CAT_COLS, "A) usuario, SIN cluster")
    evaluate(
        clustered,
        NUM_COLS + CLUSTER_NUM,
        CAT_COLS + CLUSTER_CAT,
        "B) usuario + cluster + class_weight",
    )
    evaluate(
        clustered,
        CLUSTER_NUM,
        CAT_COLS + CLUSTER_CAT,
        "C) sin lat/long crudos (anti-memorización)",
    )


if __name__ == "__main__":
    main()