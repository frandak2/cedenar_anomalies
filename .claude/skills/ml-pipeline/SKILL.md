---
name: ml-pipeline
description: Úsalo al entrenar o ejecutar inferencia de los modelos de anomalías (fuzzy-c-means + LightGBM por región), preparar datasets, o gestionar los artefactos models/*.pkl de cedenar_anomalies.
---

# Skill: Pipeline ML (entrenamiento e inferencia)

Guía para operar el pipeline de detección de anomalías de `cedenar_anomalies`.

## Modelos y artefactos

- `models/pipeline_<REGION>.pkl` con REGION ∈ {CENTRO, NORTE, OCCIDENTE, PACIFICO, SUR}:
  pipelines de **clustering difuso (fuzzy-c-means)** por región.
- `models/pipe_puntaje.pkl`: pipeline de **clasificación/puntaje** (LightGBM).
- Métricas: `models/metrics_class_puntaje.csv`, `models/euclidean_silhouette_scores.csv`.
- Acceso a rutas: usa `cedenar_anomalies.utils.config.MODELS_DIR` (no rutas absolutas).

## Código relevante

- Dominio: `domain/models/sklearn_fcm_wrapper.py`, `domain/services/clustering_pipeline_service.py`,
  `domain/services/data_processing_service.py`, `domain/services/data_cleaning_service.py`,
  `domain/services/inference_service.py`.
- Aplicación (ejecutables): `application/make_train_dataset.py`, `application/train.py`,
  `application/make_inference_dataset.py`, `application/inference.py`.

## Flujo de ENTRENAMIENTO

```bash
bash PipelineExecutionTrain.sh
# Equivale a, en orden:
poetry run python cedenar_anomalies/application/make_train_dataset.py
poetry run python cedenar_anomalies/application/train.py
```
Los logs se anexan a `pipeline_execution_train.log`.

## Flujo de INFERENCIA

```bash
bash PipelineExecutionInference.sh
# Equivale a, en orden:
poetry run python cedenar_anomalies/application/make_inference_dataset.py
poetry run python cedenar_anomalies/application/inference.py
poetry run python cedenar_anomalies/application/send_to_BQ_inference.py
```
Los logs se anexan a `pipeline_execution_inference.log`.

> Aviso: el archivo `PipelineExecutionInference.sh` referencia `send_to_BQ_infernce.py` (typo).
> El archivo real es `send_to_BQ_inference.py`. Si el script falla por "Script not found",
> corrige el nombre en `PipelineExecutionInference.sh` (commit `fix(ml): ...`).

## Buenas prácticas

- Entrena/infiere SIEMPRE con `poetry run` (respeta el entorno y la estructura del proyecto).
- No sobrescribas `data/raw/`. Los datasets generados van a `data/interim` / `data/processed`.
- Si cambias features o el wrapper FCM, re-genera métricas y revisa `silhouette_scores`.
- Versiona cambios de pipeline con ámbito `ml` en el commit.
- Para cambios no triviales en el pipeline, primero usa el flujo SDD (`/sdd-spec`).
