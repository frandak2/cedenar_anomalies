# Regla: Datos y modelos

## Estructura de datos
- `data/raw/` — datos originales inmutables (NO modificar ni sobrescribir).
- `data/interim/` — datos intermedios transformados.
- `data/processed/` — datasets finales para modelado.
- `data/external/` — datos de terceros.
- Accede siempre con `utils.config.RAW_DATA_DIR` / `PROCESSED_DATA_DIR` o
  `utils.paths.data_dir("raw", ...)`. Nunca con rutas absolutas.

## Modelos ML
- Los modelos entrenados viven en `models/` como `*.pkl`:
  - `pipe_puntaje.pkl` — pipeline de puntaje/clasificación.
  - `pipeline_<REGION>.pkl` con REGION ∈ {CENTRO, NORTE, OCCIDENTE, PACIFICO, SUR} — pipelines de
    clustering difuso por región.
  - Métricas en `models/metrics_class_puntaje.csv`, `models/euclidean_silhouette_scores.csv`.
- Carga/guarda modelos relativos a `utils.config.MODELS_DIR`.

## Control de versiones
- `.gitignore` excluye por defecto `*.csv`, `*.xlsx`, `*.txt`, `*.json`, `*.pkl`. Por tanto los
  datos y modelos NO se commitean salvo que se fuerce con `git add -f` (hazlo solo si es
  intencional y el archivo es pequeño y necesario, p. ej. `category_mappings_anomaly_ext.json`).
- NO commitees logs (`*.log` está ignorado), `postgresql/data/`, `.env`, ni `.idea/`.
