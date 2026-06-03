# CLAUDE.md — cedenar_anomalies

Guía para Claude Code al trabajar en este repositorio. Responde **en español**.

## Qué es este proyecto

Análisis y detección de **anomalías** de consumo eléctrico (Zentry / CEDENAR) mediante
**clustering difuso (fuzzy-c-means)** y clasificación con **LightGBM**, con pipelines por región
(CENTRO, NORTE, OCCIDENTE, PACIFICO, SUR). Integra Postgres, BigQuery y Google Sheets, y expone
datos a Looker vía un túnel ngrok.

## Stack

- **Python 3.12** + **Poetry** (entorno y dependencias). Punto de entrada: `cedenar` (script poetry).
- ML: `scikit-learn`, `fuzzy-c-means`, `lightgbm`, `optuna`.
- Datos: `pandas`, `pyjanitor`, `openpyxl`.
- Persistencia: `SQLAlchemy` 2.x (async `asyncpg` + sync `psycopg2`), `Alembic`.
- Nube/integraciones: `google-cloud-bigquery`, `pandas-gbq`, `gspread`, `oauth2client`, `aiohttp`.
- Calidad: `ruff`, `pylint`, `bandit`, `mypy`, `pre-commit` (conventional commits).

## Arquitectura (hexagonal)

```
cedenar_anomalies/
├── domain/         # negocio puro: models/, ports/ (interfaces ABC), services/
├── application/    # casos de uso ejecutables (make_*_dataset, train, inference, send_to_BQ_*, load_*)
├── infrastructure/ # adaptadores: database/, adapters/{repositories,api,ngrok_connector}
└── utils/          # config.py, paths.py, logging_config.py, async_alembic.py
```

Detalle y reglas de dependencia: ver `@.claude/rules/arquitectura-hexagonal.md`.

## Comandos frecuentes

```bash
# Entorno
poetry install                      # instalar dependencias
poetry run python <ruta>            # ejecutar un script

# Pipelines (scripts orquestadores en bash)
bash PipelineExecutionTrain.sh      # make_train_dataset.py -> train.py
bash PipelineExecutionInference.sh  # make_inference_dataset.py -> inference.py -> send_to_BQ_inference.py

# Base de datos / migraciones
poetry run alembic upgrade head     # aplicar migraciones
poetry run alembic revision --autogenerate -m "<mensaje>"

# Calidad
poetry run pre-commit run --all-files
poetry run ruff check . && poetry run ruff format .

# Jupyter
poetry run invoke lab               # lanzar Jupyter Lab
```

## Convenciones

Las reglas detalladas están en `.claude/rules/` y se cargan a continuación:

- @.claude/rules/arquitectura-hexagonal.md
- @.claude/rules/estilo-python.md
- @.claude/rules/datos-y-modelos.md
- @.claude/rules/git-y-commits.md

## Subagentes disponibles

- `arquitecto-hexagonal` — valida que el código respete los límites de capas.
- `revisor-calidad` — corre ruff/pylint/mypy/bandit y revisa el diff.

## Skills propios

- `ml-pipeline` — entrenar/inferir modelos por región y gestionar `models/*.pkl`.
- `migraciones-alembic` — crear/aplicar migraciones (async + sync).
- `datos-externos` — cargas a BigQuery / Google Sheets y túnel ngrok para Looker.
- `sdd-flujo` — flujo Spec-Driven Development sobre superpowers.

## Flujo de trabajo: Spec-Driven Development (SDD)

Para cualquier cambio no trivial sigue el flujo SDD (ver `.claude/specs/README.md`):
`/sdd-spec` (especificación) → `/sdd-plan` (plan) → `/sdd-tasks` (tareas) → ejecución con
superpowers:executing-plans. Las plantillas viven en `.claude/specs/templates/`.

## Notas importantes

- NO leas ni expongas `.env` (contiene credenciales de DB/API/Google).
- `data/` y `models/*.pkl` están fuera del control de versiones por defecto.
- Usa Conventional Commits; `git push` requiere confirmación explícita del usuario.
