# Etapa C — Alinear la inferencia al modelo de riesgo por usuario

> Plan derivado (la Etapa C del plan de mejora de performance se difería a "un plan aparte tras validar A/B"). A y B ya están validadas/desplegadas (ROC-AUC ~0.87). Ejecutar con `venv/bin/python`. Commits sin línea de co-autor. **No hacer `git push`. El envío a BigQuery trunca producción → requiere confirmación explícita aparte.**

> **ESTADO: ✅ EJECUTADO (2026-06-04).** Inferencia a nivel usuario desplegada y **BigQuery cargado** (537.533 filas en `Datos_Inference`); rama `dev` pusheada. **Desviación respecto a este plan:** el esquema de BQ **NO se redujo** — el contrato del dashboard de Looker exige conservar todas las columnas. La salida final tiene **19 columnas** (incluidas `Ejecucion`, `kWh Rec`, `Nombre`, `BARRIO_PRODUCTO`, `MUNICIPIO_PRODUCTO`, `SECCIONAL`); `kWh Rec` = **suma histórica por usuario**. Detalle en `docs/2026-06-04-rediseno-modelo-riesgo-usuario.md`.

**Goal:** Que la inferencia produzca un **score de riesgo por usuario** (severidad máx 1-5 + propensiones `puntaje_1..5`) coherente con el modelo entrenado en la Etapa B (nivel usuario, features de cluster, `class_weight='balanced'`).

## Context

Tras el rediseño, el modelo entrena a nivel **usuario** (`02_dataset_train_user.csv`, target = max puntaje) con features = perfil + `cluster_id`/`cluster_0/1/2`. La inferencia actual quedó **desalineada**:
- `make_inference_dataset.py` produce filas **por anomalía** (right-join anomalías→usuarios) y usa el archivo viejo `anomalias 2022 23 y 24.xlsx`.
- `inference.py` predice por fila y no agrega a nivel usuario.

Para un mapa de riesgo, lo correcto es **1 fila por usuario** del maestro, con su perfil, clusterizado, y el modelo de puntaje aplicado.

## Decisión de diseño (a confirmar antes de ejecutar)

**¿Qué universo se scorea en inferencia?**
- (Recomendado) **Todos los usuarios del maestro** `cedenar_data.xlsx` (mapa de riesgo completo para priorizar inspecciones), no solo los que ya tienen anomalía. Esto evita el sesgo de selección del entrenamiento.
- Alternativa: solo usuarios presentes en un archivo de anomalías (universo reducido).

El resto del plan asume el universo = **todos los usuarios del maestro**.

## Mapa de archivos

- Modificar: `cedenar_anomalies/application/make_inference_dataset.py` — generar `dataset_to_inference.csv` a **nivel usuario** (1 fila/usuario del maestro con las 12 columnas de perfil), sin necesidad de target/ponderado.
- Modificar: `cedenar_anomalies/application/inference.py` — cargar pipelines de cluster (`load_pipelines`) → `predict_all_zones` (añade `cluster_id`+`cluster_0/1/2`) → cargar `pipe_puntaje.pkl` → `predict` (añade `puntaje_pred` + `puntaje_1..5`). Salida 1 fila/usuario.
- Sin cambios de esquema en `send_to_BQ_inference.py` salvo que las columnas de salida ya quedan a nivel usuario (revisar `cols_sheet`/`column_order_for_bq`).

Reutiliza: `DataProcessingService` (solo para limpieza de perfil de usuarios si hace falta), `PipelineClusterFzz.load_pipelines/predict_all_zones`, `PipelinePuntaje(use_cluster_features=True).load_pipeline/predict`.

## Tareas

### C1: `make_inference_dataset.py` a nivel usuario
- Leer `cedenar_data.xlsx`, seleccionar/renombrar las 12 columnas de perfil que usa el modelo (`LATI_USU, LONG_USU, LATI_TRAFO, LONG_TRAFO, TRAFO_OPEN, FASES, KVA, AREA, PLAN_COMERCIAL, ZONA, CATEGORIA, SUB_CATEGORIA`) + identificador `Usuario` (= `PRODUCTO`).
- Filtrar filas sin `ZONA` y zonas con <100 registros (consistente con entrenamiento).
- Deduplicar a 1 fila por usuario.
- Guardar `data/interim/dataset_to_inference.csv`.
- Verificar: nº filas = nº usuarios únicos; 5 zonas; columnas de perfil presentes.

### C2: `inference.py` a nivel usuario
- Cargar `dataset_to_inference.csv`.
- `PipelineClusterFzz().load_pipelines()` → `predict_all_zones(df, pipelines)` para añadir features de cluster.
- `PipelinePuntaje(use_cluster_features=True).load_pipeline()` → `predict(pipeline, df_con_cluster)`.
- Construir salida a nivel usuario: `Usuario, ZONA, AREA, PLAN_COMERCIAL, LATI_USU, LONG_USU, cluster_id, puntaje (=puntaje_pred), puntaje_1..5` y guardar en `data/interim/dataset_inference.csv` + `data/processed/`.
- Verificar: 1 fila/usuario; `puntaje` en 1-5; propensiones suman ~1.

### C3: Validación local (sin BigQuery)
- Correr C1 y C2; revisar distribución de `puntaje` predicho y nº de usuarios de alto riesgo (puntaje 4-5).
- Comparar el conteo de alto riesgo con expectativas de negocio.

### C4 (GATED — no ejecutar sin OK explícito): envío a BigQuery
- `send_to_BQ_inference.py` hace `WRITE_TRUNCATE` sobre `proyecto-ia-462422.Datos_IA_LK.Datos_Inference` (producción, alimenta Looker). Ajustar `column_order_for_bq`/schema al esquema de salida a nivel usuario y, **solo con confirmación explícita**, ejecutar.

## Verificación end-to-end
1. `dataset_to_inference.csv` con 1 fila por usuario del maestro y 5 zonas.
2. `inference.py` corre sin traceback y produce `dataset_inference.csv` a nivel usuario con `puntaje` 1-5 + propensiones.
3. Distribución de riesgo razonable; usuarios alto riesgo identificados para priorización.
4. BigQuery NO tocado hasta confirmación explícita.

## Riesgos
- **Sesgo de selección**: el modelo se entrenó con usuarios que ya tenían anomalía; al scorear todo el maestro, las predicciones para usuarios muy distintos al universo de entrenamiento son extrapolación. Documentar y monitorear.
- **Compatibilidad dashboard**: mantener nombres de columnas `puntaje`, `puntaje_1..5`, `cluster_id` para no romper Looker.
- **Coordenadas faltantes**: filtrar/imputar usuarios sin `LATI_USU/LONG_USU` antes de clusterizar.
