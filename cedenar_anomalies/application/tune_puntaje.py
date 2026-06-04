# cedenar_anomalies/application/tune_puntaje.py
"""Tuning de hiperparámetros (Optuna) para PipelinePuntaje (LightGBM multiclase).

Reutiliza PipelinePuntaje.build_pipeline() para que los mejores parámetros sean
óptimos exactamente para el pipeline de producción. Replica la búsqueda del
notebook LGBM_class.ipynb: 5-fold StratifiedKFold, ROC-AUC weighted OVR y
penalización de overfitting (alpha=0.3).
"""

import argparse
import json
import logging

import optuna
import pandas as pd
from sklearn.base import clone
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.preprocessing import LabelEncoder

from cedenar_anomalies.domain.services.clustering_pipeline_service import (
    PipelinePuntaje,
)
from cedenar_anomalies.utils.paths import data_interim_dir

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("tune_puntaje")

N_SPLITS = 5
ALPHA = 0.3


def build_objective(x_train, y_train_encoded):
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 700),
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 30, 300),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
            "max_bin": trial.suggest_int("max_bin", 100, 300),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True),
            "min_gain_to_split": trial.suggest_float("min_gain_to_split", 0, 15),
            "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
            "bagging_freq": trial.suggest_int("bagging_freq", 1, 10),
            "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
        }
        base_pipe = PipelinePuntaje(params=params, logger=logger).build_pipeline()
        skf = StratifiedKFold(n_splits=N_SPLITS, shuffle=True, random_state=42)
        scores = []
        for train_idx, fold_idx in skf.split(x_train, y_train_encoded):
            x_tr, x_fold = x_train.iloc[train_idx], x_train.iloc[fold_idx]
            y_tr, y_fold = y_train_encoded[train_idx], y_train_encoded[fold_idx]
            model = clone(base_pipe)
            model.fit(x_tr, y_tr)
            auc_train = roc_auc_score(
                y_tr,
                model.predict_proba(x_tr),
                multi_class="ovr",
                average="weighted",
            )
            auc_fold = roc_auc_score(
                y_fold,
                model.predict_proba(x_fold),
                multi_class="ovr",
                average="weighted",
            )
            scores.append((auc_train, auc_fold))
        scores_df = pd.DataFrame(scores, columns=["train_score", "fold_score"])
        fold_mean = scores_df.fold_score.mean()
        fold_std = scores_df.fold_score.std()
        if pd.isna(fold_std):
            fold_std = 0.0
        overfit_penalty = (scores_df.train_score.mean() - fold_mean) * ALPHA
        return fold_mean - (fold_std * (1 - ALPHA)) - overfit_penalty

    return objective


def callback(study, trial):
    if trial.number > 0 and trial.number % 10 == 0 and study.best_trial:
        logger.info(
            "Tras %d trials | best value: %s | best trial: %d",
            trial.number,
            round(study.best_value, 4),
            study.best_trial.number,
        )


def main():
    parser = argparse.ArgumentParser(description="Tuning Optuna para PipelinePuntaje")
    parser.add_argument("--trials", type=int, default=300, help="Número de trials Optuna")
    args = parser.parse_args()

    df = pd.read_csv(data_interim_dir("01_dataset_train_clean.csv"))
    df = df.dropna(subset=["puntaje"])
    logger.info("Dataset de tuning cargado. Shape: %s", df.shape)

    df["puntaje_zona_stratify"] = df["puntaje"].astype(str) + "_" + df["ZONA"].astype(str)
    y = df["puntaje"].astype(int)
    x_train, _, y_train, _ = train_test_split(
        df,
        y,
        test_size=0.20,
        random_state=42,
        stratify=df["puntaje_zona_stratify"],
    )
    y_train_encoded = LabelEncoder().fit_transform(y_train)
    logger.info("X_train para tuning: %s", x_train.shape)

    optuna.logging.get_logger("optuna").setLevel(logging.WARNING)
    study = optuna.create_study(direction="maximize", study_name="LGBM Classifier")
    study.optimize(
        build_objective(x_train, y_train_encoded),
        n_trials=args.trials,
        callbacks=[callback],
    )

    logger.info("Mejor valor objetivo: %s", study.best_value)
    logger.info("Mejores parámetros: %s", study.best_params)

    out_path = data_interim_dir("best_params_puntaje.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(study.best_params, fh, indent=2)
    logger.info("best_params guardados en: %s", out_path)


if __name__ == "__main__":
    main()
