# Rediseño del modelo de anomalías → modelo de riesgo por usuario

**Fecha:** 2026-06-04 · **Rama:** `dev` (pusheada a `origin`) · **Estado:** **en producción** — modelos reentrenados, inferencia generada, tabla de BigQuery `Datos_Inference` cargada (537.533 filas) y Looker en uso.

Este documento explica **qué cambió, por qué, qué se gana, qué sesgos tiene, cómo se interpretan las predicciones y cómo/por qué cambia el esquema de BigQuery.**

---

## 1. Resumen ejecutivo

El modelo de "puntaje" rendía casi como el azar (ROC-AUC ≈ 0.51, accuracy ≈ 0.44). El diagnóstico mostró que **el problema no eran los hiperparámetros sino el diseño**: se intentaba predecir, por cada anomalía, una severidad que **está determinada por el código de la anomalía**, usando como entrada **solo atributos del usuario** (ubicación, área, plan…). Eso es información insuficiente y generaba ~98% de "colisiones" (mismas features, distinto target).

Se reformuló a un **modelo de riesgo por usuario**: una fila por usuario, target = **severidad máxima** de sus anomalías (1–5), features = perfil del usuario + **features de cluster geográfico** + **balanceo de clases**. Resultado: **ROC-AUC 0.51 → 0.87**.

La inferencia ahora produce un **mapa de riesgo por usuario** (537.533 usuarios) para priorizar inspecciones.

---

## 2. Contexto y diagnóstico (por qué se hizo)

### 2.1. Cómo funcionaba antes
- **Entrenamiento (`make_train_dataset.py` + `train.py`)**: se "explotaba" cada orden de anomalía por código, se cruzaba con la tabla `ponderado` (que asigna un `puntaje` a cada código) y con el maestro de usuarios. Cada fila = (anomalía, código) con los atributos del usuario repetidos. El modelo (LightGBM) predecía `puntaje` (1–5) a partir de atributos del usuario.
- **Inferencia**: igual, por anomalía, y se enviaba a BigQuery (`Datos_Inference`) para Looker.

### 2.2. Errores detectados (con evidencia sobre los datos)
| # | Error | Evidencia |
|---|---|---|
| 1 | **El target lo determina el código de anomalía, que NO es feature.** `puntaje` es 100% función de `id`/`Nombre` de la anomalía; el modelo solo usaba atributos del usuario. | `id`/`Nombre` → 1 solo puntaje cada uno (100% determinista). |
| 2 | **Colisiones masivas.** Mismas features → distinto `puntaje` en el 98.2% de las filas. Techo de accuracy con ese diseño: 0.47. | 16.171/19.432 grupos de features con >1 puntaje. |
| 3 | **Fuga de datos en el split.** División por fila con el mismo usuario repetido ~10 veces (máx 383) → el mismo usuario en train y test. | 20.188 usuarios / 209.172 filas. |
| 4 | **Features de cluster perdidas.** El notebook original usaba `cluster_id` + membresías FCM; producción las había perdido. | Notebook `LGBM_class.ipynb` vs `clustering_pipeline_service.py`. |
| 5 | **Sin balanceo de clases.** El notebook usaba `class_weight='balanced'`; producción no. | `build_pipeline`. |
| 6 | **Una sola partición** para reportar métricas (alta varianza). | `PipelinePuntaje.fit`. |

**Conclusión del diagnóstico:** incluso la mejor versión tuneada del notebook llegaba solo a ROC-AUC ≈ 0.56. El cuello de botella era estructural, no de tuning.

---

## 3. Qué se cambió (y por qué cada cambio)

### 3.1. Reformulación a nivel usuario *(cambio principal)*
- **Nuevo:** `cedenar_anomalies/application/make_user_dataset.py` agrega el dataset por anomalía a **1 fila por usuario**; `target = max(puntaje)` de sus anomalías. Los 12 atributos de perfil son constantes por usuario (verificado: 0 usuarios con atributos variables), así que la agregación es sin pérdida.
- **Por qué:** elimina las colisiones (cada usuario tiene un único target) y la fuga (1 fila/usuario ⇒ no hay usuarios repetidos entre train y test). Es además el enfoque coherente con el uso real: un **mapa de riesgo por usuario** para priorizar.

### 3.2. Features de cluster geográfico re-añadidas
- **Cambio:** `PipelinePuntaje` ahora acepta `use_cluster_features=True` y añade a las features: `cluster_id` (cluster duro) y `cluster_0/1/2` (membresía difusa FCM por zona).
- **Por qué:** la geografía cruda (lat/long exactas) **identifica** a cada usuario → riesgo de memorización. El cluster resume la geografía de forma **generalizable**. Permite generalizar a usuarios nuevos.

### 3.3. Balanceo de clases
- **Cambio:** `class_weight='balanced'` en el clasificador (LightGBM y el fallback RandomForest).
- **Por qué:** el target a nivel usuario está muy desbalanceado (clase 5 = 78%). Sin balanceo, el modelo ignora las clases minoritarias (3 y 4), que son justamente accionables.

### 3.4. Validación correcta
- **Nuevo:** `cedenar_anomalies/application/validate_risk.py`: validación cruzada *out-of-fold* (StratifiedKFold) que reporta accuracy, **F1 macro**, F1 weighted, **recall por clase**, ROC-AUC y el **gap train/test** (overfitting). Compara 3 variantes (con/sin cluster, con/sin lat-long).
- **Por qué:** una sola partición da estimaciones ruidosas; con desbalance, la accuracy engaña. El harness probó la mejora **antes** de tocar producción.

### 3.5. Estratificación robusta
- **Cambio:** el split pasa de estratificar por `puntaje×zona` a estratificar por `puntaje`.
- **Por qué:** a nivel usuario, combinaciones puntaje×zona tienen tan pocos casos (p.ej. `puntaje=2` tiene 9 usuarios en total) que rompían `train_test_split`/`StratifiedKFold`.

### 3.6. Exclusión de zonas diminutas
- **Cambio:** se descartan zonas con <100 registros antes de entrenar (p.ej. `COCANA`, 8 filas).
- **Por qué:** no son modelables y rompen la estratificación. En inferencia, esos usuarios simplemente no reciben score (no hay modelo de cluster para su zona).

### 3.7. Tuning Optuna como paso del pipeline
- **Cambio:** `tune_puntaje.py` (Optuna, objetivo **F1 macro**) integrado en `PipelineExecutionTrain.sh` (make_train_dataset → make_user_dataset → tune → train); `train.py` carga los `best_params` desde JSON con *fallback*.
- **Resultado:** el tuning (300 trials) dio una mejora marginal y **peor ROC-AUC** que los defaults (0.858 vs 0.870), porque optimiza macro-F1 y sacrifica ranking. **Se desplegaron los defaults**; el tuning quedó archivado (`data/interim/best_params_puntaje.tuned.json`). Confirma que la ganancia vino del rediseño, no del tuning.

### 3.8. Inferencia a nivel usuario
- **Cambio:** `make_inference_dataset.py` y `inference.py` reescritos: leen el **maestro de usuarios** `cedenar_data.xlsx`, una fila por usuario, clusterizan (`predict_all_zones`) y aplican el modelo de riesgo. Salida: 1 fila por usuario con las **16 columnas del contrato** de `Datos_Inference` (las columnas de origen-anomalía se conservan; ver §7).
- **Por qué:** alinear inferencia con el nuevo modelo y producir el mapa de riesgo de **todos** los usuarios (no solo los que ya tienen anomalía).

---

## 4. Beneficios

| Métrica (modelo de puntaje) | Antes (por anomalía) | **Ahora (riesgo por usuario)** |
|---|---|---|
| ROC-AUC (ovr, weighted) | 0.510 | **0.870** |
| Accuracy | 0.437 | 0.751 |
| F1 (weighted) | 0.299 | 0.779 |
| Precision (weighted) | 0.322 | 0.844 |
| Gap train/test | — | ~0.02 (sano, sin memorización) |

- **Poder de ranking** mucho mayor (ROC-AUC 0.87): el modelo separa bien a los usuarios de mayor severidad → priorización útil.
- **Sin fuga ni colisiones**: la evaluación es honesta.
- **Reproducible y operable**: pipeline en 3 pasos + tuning opcional, todo logueado en `pipeline_execution_train.log`.
- **Clustering sano**: silhouette 0.55–0.64 por zona.

---

## 5. Sesgos y limitaciones (importante)

1. **Sesgo de selección (el más importante).** El modelo se entrena solo con **usuarios que ya tienen anomalía detectada**, donde el 78% tiene severidad máxima 5. Por eso, al aplicar el modelo a los 537k usuarios del maestro, la mayoría sale clasificada como riesgo **4–5**. El modelo responde a *"dado que un usuario es como estos, ¿qué tan severa sería su anomalía?"*, **no** a *"¿qué probabilidad tiene de tener una anomalía?"*.
   - **Implicación:** no usar el corte de clase (4 vs 5) como verdad absoluta; **usar el ranking por propensión** (ver §6).
   - **Mejora futura:** un modelo de propensión real necesita **ejemplos negativos** (usuarios sin anomalía) y/o un enfoque temporal (features de un periodo, etiqueta del siguiente).

2. **Clases 1 y 2 casi inexistentes** (76 y 9 usuarios). El modelo prácticamente no las aprende; sus métricas por-clase son ~0. No es un error, es falta de datos.

3. **Extrapolación geográfica/categórica.** Al scorear todo el maestro, hay usuarios con perfiles o categorías poco vistos en entrenamiento; sus scores son menos confiables.

4. **Zonas sin modelo** (p.ej. COCANA) **no se scorean** (se omiten en la inferencia). En la última corrida: 561.747 usuarios candidatos → 537.533 scoreados (~24k de COCANA omitidos).

5. **Cluster ajustado sobre todo el dataset** (no dentro de cada fold) en validación/tuning. Es no supervisado (no ve el target) → sesgo despreciable y consistente entre train y servicio.

---

## 6. Cómo se interpretan las predicciones

La salida (`data/interim/dataset_inference.csv`, 1 fila por usuario) tiene:

| Columna | Significado |
|---|---|
| `Usuario` | Identificador del usuario/predio (antes `PRODUCTO`). |
| `ZONA`, `AREA`, `PLAN_COMERCIAL` | Atributos de contexto/segmentación. |
| `LATI_USU`, `LONG_USU` | Coordenadas (para el mapa). |
| `cluster_id` | Grupo geográfico FCM dentro de la zona (contexto, no severidad). |
| `puntaje` | **Severidad de riesgo predicha (1–5)**: la clase más probable. |
| `puntaje_1 … puntaje_5` | **Propensiones** (probabilidades) a cada nivel de severidad. Suman ~1. |
| `Ejecucion` | Fecha de **scoring** del riesgo (cuándo se calculó). Columna del contrato. |
| `kWh Rec` | **Suma histórica** de la energía recuperada de las anomalías del usuario (NULL si nunca tuvo). Mantiene vivas las agregaciones de kWh por cluster en Looker. |
| `Nombre` | NULL (no hay una anomalía concreta asociada). Columna del contrato. |
| `BARRIO_PRODUCTO`, `MUNICIPIO_PRODUCTO`, `SECCIONAL` | Atributos del usuario (del maestro). Dimensiones/filtros del dashboard. |
| `ubi_usu1` | **No se genera aquí**: es un campo geográfico que Looker calcula desde `LATI_USU`/`LONG_USU`. |

**Cómo priorizar inspecciones (recomendado):**
- **No** ordenar solo por `puntaje` (clase): por el sesgo de selección, casi todos son 4–5.
- **Ordenar por la propensión a alta severidad**, p.ej. `puntaje_5` (y como desempate `puntaje_4`). Eso da un ranking continuo de "más riesgoso → menos riesgoso" para enfocar cuadrillas.
- Filtrar por zona/área cuando se planifiquen rutas.

**Ejemplo:** un usuario con `puntaje=5, puntaje_5=0.86` es prioridad más alta que otro con `puntaje=5, puntaje_5=0.41`, aunque ambos compartan clase.

---

## 7. Esquema de BigQuery: se PRESERVA el contrato

> La tabla `proyecto-ia-462422.Datos_IA_LK.Datos_Inference` (la que consume Looker) se carga con `WRITE_TRUNCATE` (borra y reemplaza). **El envío YA se ejecutó** el 2026-06-04: **537.533 filas** cargadas (reemplazó las 537.524 previas), esquema de 19 columnas verificado contra la tabla viva.

### 7.1. Decisión: se preserva el contrato de columnas (verificado contra el dashboard)
Las columnas de la tabla son un **contrato** del que dependen los tableros de Looker (filtros y agregaciones por `Ejecucion`, `kWh Rec`, `Nombre`, `BARRIO_PRODUCTO`, etc.). La inferencia a nivel usuario emite **las 19 columnas que usa el dashboard**. **Hallazgo:** el `send_to_BQ_inference.py` original solo enviaba 16 columnas y **omitía** `BARRIO_PRODUCTO`/`MUNICIPIO_PRODUCTO`/`SECCIONAL` (que el dashboard sí usa) — con `WRITE_TRUNCATE` se habrían **perdido**; se añadieron. Lo que cambia respecto al flujo viejo es la **granularidad** (1 fila/usuario) y el **significado** de algunas columnas, no el set de columnas del dashboard.

### 7.2. Cómo se poblan, a nivel usuario, las columnas de origen-anomalía
| Columna | Valor a nivel usuario | Motivo |
|---|---|---|
| `Ejecucion` (DATE) | **Fecha de scoring** (cuándo se calculó el riesgo) | Mantiene utilizable el filtro/eje temporal en Looker. |
| `kWh Rec` (FLOAT) | **Suma histórica** de la energía recuperada de las anomalías del usuario (NULL si nunca tuvo) | Reproduce los totales de los gráficos de kWh por cluster (igual que cuando era por anomalía). |
| `Nombre` (STRING) | **NULL** | No hay una anomalía concreta asociada al usuario. |

Columnas de perfil del usuario que el dashboard usa como dimensiones/filtros, arrastradas desde el maestro `cedenar_data.xlsx`: `BARRIO_PRODUCTO`, `MUNICIPIO_PRODUCTO`, `SECCIONAL` (STRING).

Además, sin cambiar nombre ni tipo:
- `puntaje` (INTEGER): pasa de ser la severidad **real** de la anomalía a la severidad **predicha** del usuario (mismo dominio 1–5).
- `Cluster` (STRING): el `cluster_id` del modelo se renombra a `Cluster` en `send_to_BQ_inference.py` (igual que antes).
- `ubi_usu1`: campo geográfico **calculado por Looker** desde `LATI_USU`/`LONG_USU`; no se genera en el pipeline.

### 7.3. Esquema (19 columnas del contrato)
`Usuario` INTEGER · `Ejecucion` DATE · `AREA` STRING · `PLAN_COMERCIAL` STRING · `Nombre` STRING · `kWh_Rec` FLOAT · `Cluster` STRING · `puntaje` INTEGER · `puntaje_1..5` FLOAT · `LATI_USU` FLOAT · `LONG_USU` FLOAT · `ZONA` STRING · `BARRIO_PRODUCTO` STRING · `MUNICIPIO_PRODUCTO` STRING · `SECCIONAL` STRING.

(En el CSV de inferencia las columnas se llaman `kWh Rec` y `cluster_id`; `send_to_BQ_inference.py` las renombra a `kWh_Rec` y `Cluster`.)

### 7.4. Impacto en Looker
- **Ningún tablero se rompe**: todas las columnas del contrato siguen presentes con su nombre y tipo (incluidas las 3 que el script antes omitía).
- Filtros/ejes por `Ejecucion` funcionan (ahora marcan la fecha de cálculo del riesgo).
- Los gráficos de `kWh Rec` por cluster **siguen funcionando** (ahora suman la energía recuperada histórica por usuario). `Nombre` queda NULL (no aplica a una predicción).
- **Granularidad nueva**: 1 fila por usuario (antes varias por usuario). Revisar conteos/medidas que asumían múltiples filas por usuario (p.ej. `COUNT(*)` ahora cuenta usuarios, no anomalías).
- `puntaje` ahora es riesgo **predicho**; para priorizar, usar el ranking por propensión (`puntaje_5`) — ver §6.

---

## 8. Cómo operar

**Reentrenar (3 pasos, logue­a a `pipeline_execution_train.log`):**
```bash
bash PipelineExecutionTrain.sh            # make_train_dataset -> make_user_dataset -> tune -> train
TUNE_TRIALS=50 bash PipelineExecutionTrain.sh   # tuning más corto
```
En este entorno sin poetry, equivalentes con venv:
```bash
venv/bin/python cedenar_anomalies/application/make_train_dataset.py
venv/bin/python cedenar_anomalies/application/make_user_dataset.py
venv/bin/python cedenar_anomalies/application/tune_puntaje.py --trials 300   # opcional
venv/bin/python cedenar_anomalies/application/train.py
```
Métricas en `models/metrics_class_puntaje.csv` y `models/euclidean_silhouette_scores.csv`. Respaldo previo en `models/backup_pre_2023-2026/`.

**Inferir (mapa de riesgo, sin BigQuery):**
```bash
venv/bin/python cedenar_anomalies/application/make_inference_dataset.py
venv/bin/python cedenar_anomalies/application/inference.py
# salida: data/interim/dataset_inference.csv (1 fila/usuario)
```

**Publicar a BigQuery (EJECUTADO 2026-06-04):** `send_to_BQ_inference.py` cargó **537.533 filas** a `Datos_Inference` con el contrato de **19 columnas** (renombra `cluster_id`→`Cluster` y `kWh Rec`→`kWh_Rec`, e incluye `BARRIO_PRODUCTO`/`MUNICIPIO_PRODUCTO`/`SECCIONAL`). Re-ejecutarlo vuelve a **truncar y reemplazar** la tabla de producción.

---

## 9. Archivos y commits

**Nuevos:** `application/make_user_dataset.py`, `application/validate_risk.py`.
**Modificados:** `domain/services/clustering_pipeline_service.py` (`PipelinePuntaje`), `application/train.py`, `application/tune_puntaje.py`, `application/make_inference_dataset.py`, `application/inference.py`, `application/send_to_BQ_inference.py` (esquema de 19 columnas del contrato), `PipelineExecutionTrain.sh`.

Commits (rama `dev`, en orden, sin co-autor, sin push):

| SHA | Descripción |
|---|---|
| `ecb0f26` | dataset a nivel usuario para modelo de riesgo |
| `f79c0b6` | guard de entrada y logger `__name__` en make_user_dataset (revisión) |
| `f262abc` | harness de validación CV del modelo de riesgo por usuario |
| `66fb2d1` | modelo de riesgo a nivel usuario (cluster features + class_weight) |
| `62ec30b` | class_weight balanced en fallback RF y docstring de tuning (revisión) |
| `1aad66f` | planes de mejora de performance y etapa C inferencia (docs) |
| `847a2fb` | inferencia de riesgo a nivel usuario (perfil → cluster → puntaje) |
| `82d8649` | guard de columnas de salida y timestamp shell-safe en inferencia (revisión) |
| `0304f00` | este documento (rediseño, sesgos, interpretación, schema BQ) |
| `f2f67bb` | preservar contrato de columnas BQ en inferencia (`Ejecucion`/`kWh Rec`/`Nombre`) + doc |
| `96ebac7` | actualizar doc (historial de commits y decisiones) |
| `15e5c13` | **preservar columnas del dashboard** `BARRIO_PRODUCTO`/`MUNICIPIO_PRODUCTO`/`SECCIONAL` en inferencia y schema BQ |
| `c41de48` | `kWh Rec` = **suma histórica** por usuario (preserva gráficos kWh de Looker) |
| `2c02401` | doc: contrato de 19 columnas y kWh histórico (verificado vs dashboard) |

> **Despliegue 2026-06-04:** se ejecutó el envío a BigQuery (537.533 filas a `Datos_Inference`, `WRITE_TRUNCATE`) y se hizo `git push` de `dev` a `origin`. Los commits posteriores a `2c02401` son de documentación.

Planes relacionados: `docs/superpowers/plans/2026-06-03-mejora-performance-modelo-puntaje.md`, `docs/superpowers/plans/2026-06-04-etapa-c-inferencia-riesgo-usuario.md`.

---

## 10. Historial de decisiones clave

- **Datos de entrenamiento:** solo el archivo nuevo `anomalias 2023-2026.xlsx`.
- **Target:** severidad máxima por usuario (1–5), para mantener compatibilidad con el dashboard.
- **Tuning Optuna:** ejecutado (300 trials, macro-F1); resultó peor en ROC-AUC que los defaults → se desplegaron los **defaults** y el tuning quedó archivado.
- **Modelos de cluster `class_weight`:** balanceado tanto en LightGBM como en el fallback RandomForest.
- **Contrato de BigQuery (verificado contra el dashboard):** se preservan **las 19 columnas** que usa Looker. Al revisar el dashboard se detectó que `send_to_BQ_inference.py` omitía `BARRIO_PRODUCTO`/`MUNICIPIO_PRODUCTO`/`SECCIONAL` (riesgo latente con `WRITE_TRUNCATE`) → se añadieron. `kWh Rec` se rellena con la **suma histórica** por usuario para mantener los gráficos de kWh por cluster. Cambia la granularidad (1 fila/usuario) y el significado de `puntaje`, no el set de columnas.
- **Despliegue final (2026-06-04):** envío a BigQuery ejecutado (`Datos_Inference`, 537.533 filas, `WRITE_TRUNCATE`) y rama `dev` pusheada a `origin`. Modelos previos respaldados en `models/backup_pre_2023-2026/` (rollback disponible).
