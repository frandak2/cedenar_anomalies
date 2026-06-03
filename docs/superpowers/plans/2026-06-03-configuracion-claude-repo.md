# Configuración de `.claude` para cedenar_anomalies — Plan de Implementación

> **Para trabajadores agénticos:** SUB-SKILL REQUERIDA: Usa superpowers:subagent-driven-development (recomendado) o superpowers:executing-plans para ejecutar este plan tarea por tarea. Los pasos usan sintaxis de checkbox (`- [ ]`) para seguimiento.

**Goal:** Dejar el repo `cedenar_anomalies` totalmente configurado para Claude Code con `CLAUDE.md`, `settings.json`, reglas, agentes, skills propios y un flujo de Spec-Driven Development (SDD) montado sobre superpowers.

**Architecture:** Toda la configuración vive bajo `.claude/` (más `CLAUDE.md` en la raíz como memoria de proyecto). Las **reglas** son archivos markdown importados desde `CLAUDE.md` con la sintaxis `@`. Los **agentes** son subagentes especializados (`.claude/agents/*.md`). Los **skills** son flujos invocables (`.claude/skills/<nombre>/SKILL.md`). El **SDD** se implementa como un skill orquestador + plantillas en `.claude/specs/` + slash-commands en `.claude/commands/`, reutilizando los skills de superpowers (brainstorming → writing-plans → executing-plans).

**Tech Stack:** Claude Code (CLAUDE.md, settings.json, agents, skills, commands), Markdown + YAML frontmatter, JSON. El repo objetivo es Python 3.12 / Poetry, arquitectura hexagonal, fuzzy-c-means + LightGBM, Postgres + Alembic (async/sync), BigQuery + Google Sheets, pre-commit (ruff, pylint, bandit, mypy, conventional-commits).

**Idioma:** Toda la configuración se redacta en **español** (coherente con el dominio y los docstrings del repo).

---

## Mapa de archivos (qué se crea y de qué es responsable)

```
CLAUDE.md                                  # Memoria de proyecto: stack, comandos, convenciones, imports de reglas
.claude/
├── settings.json                          # Permisos de herramientas + protección de .env
├── rules/
│   ├── arquitectura-hexagonal.md          # Límites domain/application/infrastructure y puertos
│   ├── estilo-python.md                   # Ruff/pylint/mypy/bandit, tipado, logging, español
│   ├── datos-y-modelos.md                 # data/, models/*.pkl, rutas con utils.paths, no commitear datos
│   └── git-y-commits.md                   # Conventional commits, ramas, pre-commit
├── agents/
│   ├── arquitecto-hexagonal.md            # Subagente revisor de límites de arquitectura
│   └── revisor-calidad.md                 # Subagente que corre ruff/pylint/mypy/bandit y revisa el diff
├── skills/
│   ├── ml-pipeline/SKILL.md               # Entrenamiento/inferencia fuzzy-c-means + LightGBM por región
│   ├── migraciones-alembic/SKILL.md       # Crear/aplicar migraciones async+sync
│   ├── datos-externos/SKILL.md            # Cargas BigQuery / Google Sheets / túnel ngrok Looker
│   └── sdd-flujo/SKILL.md                 # Orquestador SDD sobre superpowers
├── commands/
│   ├── sdd-spec.md                        # /sdd-spec  → crea una especificación
│   ├── sdd-plan.md                        # /sdd-plan  → deriva el plan desde la spec
│   └── sdd-tasks.md                       # /sdd-tasks → deriva tareas ejecutables desde el plan
└── specs/
    ├── README.md                          # Cómo funciona el flujo SDD en este repo
    └── templates/
        ├── spec-template.md               # Plantilla de especificación
        ├── plan-template.md               # Plantilla de plan
        └── tasks-template.md              # Plantilla de tareas
```

**Nota sobre TDD:** Este plan crea **archivos de configuración**, no código de producción, por lo que no hay tests unitarios. En lugar de "test que falla → implementación → test que pasa", cada tarea usa el ciclo: **crear archivo → verificar (existencia/JSON válido/contenido) → commit**. Las verificaciones son comandos reales con salida esperada.

---

## Tarea 1: Estructura base de `.claude` y `settings.json`

**Files:**
- Create: `.claude/settings.json`
- Create (carpetas): `.claude/rules/`, `.claude/agents/`, `.claude/skills/`, `.claude/commands/`, `.claude/specs/templates/`

- [ ] **Step 1: Crear el árbol de carpetas**

Run:
```bash
mkdir -p .claude/rules .claude/agents \
  .claude/skills/ml-pipeline .claude/skills/migraciones-alembic \
  .claude/skills/datos-externos .claude/skills/sdd-flujo \
  .claude/commands .claude/specs/templates
```
Expected: sin salida (éxito).

- [ ] **Step 2: Crear `.claude/settings.json`**

Contenido completo de `.claude/settings.json`:
```json
{
  "$schema": "https://json.schemastore.org/claude-code-settings.json",
  "permissions": {
    "allow": [
      "Bash(poetry run:*)",
      "Bash(poetry install:*)",
      "Bash(poetry add:*)",
      "Bash(poetry lock:*)",
      "Bash(pre-commit run:*)",
      "Bash(ruff:*)",
      "Bash(pylint:*)",
      "Bash(mypy:*)",
      "Bash(bandit:*)",
      "Bash(alembic upgrade:*)",
      "Bash(alembic downgrade:*)",
      "Bash(alembic revision:*)",
      "Bash(alembic current:*)",
      "Bash(alembic history:*)",
      "Bash(invoke:*)",
      "Bash(git status:*)",
      "Bash(git diff:*)",
      "Bash(git log:*)",
      "Bash(git add:*)",
      "Bash(git commit:*)",
      "Read(//Users/frandak2/Documents/repos/cedenar_anomalies/**)"
    ],
    "deny": [
      "Read(./.env)",
      "Read(./.env.*)"
    ],
    "ask": [
      "Bash(git push:*)",
      "Bash(alembic downgrade base:*)"
    ]
  },
  "enableAllProjectMcpServers": false
}
```

- [ ] **Step 3: Verificar que el JSON es válido**

Run: `python -c "import json; json.load(open('.claude/settings.json')); print('JSON OK')"`
Expected: `JSON OK`

- [ ] **Step 4: Verificar el árbol creado**

Run: `find .claude -type d | sort`
Expected (orden alfabético):
```
.claude
.claude/agents
.claude/commands
.claude/rules
.claude/skills
.claude/skills/datos-externos
.claude/skills/migraciones-alembic
.claude/skills/ml-pipeline
.claude/skills/sdd-flujo
.claude/specs
.claude/specs/templates
```

- [ ] **Step 5: Commit**

```bash
git add .claude/settings.json
git commit -m "chore(claude): añadir estructura base de .claude y settings.json"
```

---

## Tarea 2: Reglas de arquitectura, estilo, datos y git

**Files:**
- Create: `.claude/rules/arquitectura-hexagonal.md`
- Create: `.claude/rules/estilo-python.md`
- Create: `.claude/rules/datos-y-modelos.md`
- Create: `.claude/rules/git-y-commits.md`

- [ ] **Step 1: Crear `.claude/rules/arquitectura-hexagonal.md`**

```markdown
# Regla: Arquitectura hexagonal

El paquete `cedenar_anomalies/` sigue arquitectura hexagonal con tres capas:

- `domain/` — Lógica de negocio pura. Contiene:
  - `domain/models/` — modelos de dominio (p. ej. `sklearn_fcm_wrapper.py`).
  - `domain/ports/repositories.py` — **interfaces** (`ABC`) que definen contratos
    (`IAnomaliaRepository`, `IApiClient`). El dominio depende de estos puertos, no de
    implementaciones concretas.
  - `domain/services/` — servicios de negocio (clustering, limpieza, inferencia, etc.).
- `application/` — Casos de uso y orquestación (scripts ejecutables: `make_train_dataset.py`,
  `train.py`, `inference.py`, `make_inference_dataset.py`, `send_to_BQ_*.py`, `load_*.py`).
- `infrastructure/` — Adaptadores concretos: `database/` (SQLAlchemy, sesiones, modelos ORM),
  `adapters/repositories/` (implementan los puertos), `adapters/api/`, `adapters/ngrok_connector/`.

## Reglas de dependencia (obligatorias)

1. `domain/` NO importa de `application/` ni de `infrastructure/`.
   - Excepción histórica existente: `domain/ports/repositories.py` importa
     `infrastructure.database.models.AnomaliaData` como tipo. No amplíes este acoplamiento;
     si añades puertos nuevos, usa tipos del dominio o `typing.Any`/genéricos.
2. `infrastructure/adapters/repositories/` DEBE implementar las interfaces de
   `domain/ports/repositories.py` (heredar de `IAnomaliaRepository` / `IApiClient`).
3. `application/` orquesta: instancia adaptadores de `infrastructure/` y servicios de `domain/`,
   nunca contiene SQL crudo ni llamadas HTTP directas (eso vive en `infrastructure/`).
4. Toda nueva integración externa (DB, API, archivos, BQ, GSheets) entra por un adaptador en
   `infrastructure/adapters/` detrás de un puerto en `domain/ports/`.

## Al revisar o escribir código

Si una tarea cruza estos límites, detente y propón la ubicación correcta antes de editar.
Para validar límites, usa el subagente `arquitecto-hexagonal`.
```

- [ ] **Step 2: Crear `.claude/rules/estilo-python.md`**

```markdown
# Regla: Estilo y calidad de Python

- **Python 3.12**, gestionado con **Poetry**. Ejecuta scripts con `poetry run python <ruta>`.
- **Idioma**: nombres de dominio, docstrings y mensajes de log en **español** (sigue el código
  existente). El código y las palabras clave en inglés.
- **Formato y lint**: `ruff` (línea máx. **90**, `target-version = py312`, reglas
  `I, E501, E4, E7, E9, F, C901`). Formatea con `ruff format`.
- **Análisis estático**: `pylint` con `.code_quality/.pylintrc` (deshabilita C0111, C0103, C0303,
  R0903, R0913, W0511, W1203).
- **Tipado**: `mypy` con `.code_quality/mypy.ini` (`warn_return_any = True`,
  `disallow_untyped_defs = False` — el tipado es gradual; añade type hints en código nuevo).
- **Seguridad**: `bandit` con `.code_quality/bandit.yaml` (skips B101, B311).
- **Logging**: usa `logging` (patrón `self.logger = logger or logging.getLogger(__name__)` como en
  `domain/services/inference_service.py`). NO uses `print` en código de librería.
- **Rutas**: usa `cedenar_anomalies.utils.config` (`BASE_DIR`, `DATA_DIR`, `MODELS_DIR`, etc.) y
  `cedenar_anomalies.utils.paths` (`data_dir()`, `models_dir()`, ...). NO hardcodees rutas absolutas.
- **Config/secretos**: lee variables con `os.getenv` vía `utils/config.py` (que hace
  `load_dotenv()`). NUNCA imprimas ni commitees el contenido de `.env`.

## Antes de dar por terminado un cambio de Python

Corre, y deja pasar, los hooks relevantes:
```bash
poetry run ruff check . && poetry run ruff format --check . && \
poetry run mypy --config-file=.code_quality/mypy.ini cedenar_anomalies
```
O delega la revisión completa al subagente `revisor-calidad`.
```

- [ ] **Step 3: Crear `.claude/rules/datos-y-modelos.md`**

```markdown
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
```

- [ ] **Step 4: Crear `.claude/rules/git-y-commits.md`**

```markdown
# Regla: Git y commits

- **Conventional Commits obligatorio** (lo valida el hook `conventional-pre-commit` en
  `commit-msg`). Formato: `<tipo>(<ámbito opcional>): <descripción>`.
  - Tipos: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `build`, `ci`, `perf`, `style`.
  - Ejemplos del repo: `feat: add puntaje columns`, `chore(claude): ...`.
- **Ámbitos sugeridos** para este repo: `claude`, `domain`, `application`, `infra`, `db`, `ml`,
  `etl`, `bq`, `gsheets`, `docs`.
- **pre-commit**: antes de commitear, los hooks corren ruff (`--fix`), ruff-format, pylint, bandit
  y mypy sobre archivos Python (excluye `venv/` y notebooks `.ipynb`). Si fallan, corrige y reintenta.
- **Ramas**: la rama principal es `main`; el trabajo activo va en `dev` o ramas `feature/<nombre>`.
  No commitees directo a `main`; abre PR.
- **`git push` requiere confirmación** (configurado en `ask` de settings.json). No hagas push sin
  que el usuario lo pida explícitamente.
- Cierra los mensajes de commit generados por Claude con la línea de coautoría correspondiente.
```

- [ ] **Step 5: Verificar los 4 archivos de reglas**

Run: `ls -1 .claude/rules/`
Expected:
```
arquitectura-hexagonal.md
datos-y-modelos.md
estilo-python.md
git-y-commits.md
```

- [ ] **Step 6: Commit**

```bash
git add .claude/rules/
git commit -m "docs(claude): añadir reglas de arquitectura, estilo, datos y git"
```

---

## Tarea 3: `CLAUDE.md` raíz (memoria de proyecto con imports de reglas)

**Files:**
- Create: `CLAUDE.md`

- [ ] **Step 1: Crear `CLAUDE.md` en la raíz del repo**

```markdown
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
```

- [ ] **Step 2: Verificar que los imports de reglas apuntan a archivos existentes**

Run:
```bash
for f in arquitectura-hexagonal estilo-python datos-y-modelos git-y-commits; do
  test -f ".claude/rules/$f.md" && echo "OK $f" || echo "FALTA $f";
done
```
Expected:
```
OK arquitectura-hexagonal
OK estilo-python
OK datos-y-modelos
OK git-y-commits
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): añadir CLAUDE.md como memoria de proyecto"
```

---

## Tarea 4: Agente `arquitecto-hexagonal`

**Files:**
- Create: `.claude/agents/arquitecto-hexagonal.md`

- [ ] **Step 1: Crear `.claude/agents/arquitecto-hexagonal.md`**

```markdown
---
name: arquitecto-hexagonal
description: Úsalo para validar que un cambio respeta los límites de la arquitectura hexagonal (domain/application/infrastructure) y los puertos. Invócalo al revisar PRs, al mover código entre capas o al añadir integraciones externas.
tools: Read, Grep, Glob, Bash
model: sonnet
---

Eres un revisor de arquitectura para el repo `cedenar_anomalies`, que sigue arquitectura hexagonal.
Respondes en español y eres conciso pero riguroso.

## Capas y reglas de dependencia

- `cedenar_anomalies/domain/` (models, ports, services): negocio puro. NO debe importar de
  `application/` ni de `infrastructure/`. (Excepción histórica tolerada: `domain/ports/repositories.py`
  importa `infrastructure.database.models.AnomaliaData` como tipo; no amplíes ese acoplamiento.)
- `cedenar_anomalies/infrastructure/adapters/repositories/`: DEBE implementar las interfaces de
  `domain/ports/repositories.py` (`IAnomaliaRepository`, `IApiClient`).
- `cedenar_anomalies/application/`: orquesta dominio + infraestructura; sin SQL crudo ni HTTP directo.
- Toda integración externa entra por un adaptador en `infrastructure/adapters/` detrás de un puerto.

## Qué hacer cuando te invocan

1. Identifica los archivos cambiados o señalados (usa `git diff` / `git status` si aplica).
2. Para cada archivo de `domain/`, busca imports prohibidos:
   `grep -rn "from cedenar_anomalies.application" cedenar_anomalies/domain` y
   `grep -rn "from cedenar_anomalies.infrastructure" cedenar_anomalies/domain`
   (la única coincidencia aceptable es el import de `AnomaliaData` ya existente).
3. Verifica que los repositorios concretos hereden de los puertos correspondientes.
4. Verifica que `application/` no contenga SQL crudo (`grep -rn "SELECT \|INSERT \|sessionmaker\|create_engine" cedenar_anomalies/application`) ni llamadas HTTP directas.

## Salida

Entrega un informe con:
- **Veredicto**: CUMPLE / NO CUMPLE.
- **Violaciones**: archivo:línea, regla incumplida y por qué.
- **Recomendación**: dónde debería vivir el código y cómo refactorizar (puerto + adaptador).

No edites código: solo diagnostica y recomienda.
```

- [ ] **Step 2: Verificar el frontmatter del agente**

Run: `head -6 .claude/agents/arquitecto-hexagonal.md`
Expected: muestra las líneas `---`, `name: arquitecto-hexagonal`, `description: ...`, `tools: Read, Grep, Glob, Bash`, `model: sonnet`, `---`.

- [ ] **Step 3: Commit**

```bash
git add .claude/agents/arquitecto-hexagonal.md
git commit -m "feat(claude): añadir subagente arquitecto-hexagonal"
```

---

## Tarea 5: Agente `revisor-calidad`

**Files:**
- Create: `.claude/agents/revisor-calidad.md`

- [ ] **Step 1: Crear `.claude/agents/revisor-calidad.md`**

```markdown
---
name: revisor-calidad
description: Úsalo para revisar la calidad de cambios de Python antes de commitear o abrir PR. Corre ruff, mypy, pylint y bandit con las configs del repo y resume los hallazgos con prioridad.
tools: Read, Grep, Glob, Bash
model: sonnet
---

Eres un revisor de calidad de código para `cedenar_anomalies` (Python 3.12, Poetry). Respondes en
español. Tu objetivo es que el cambio pase pre-commit a la primera.

## Herramientas y configs del repo

- Formato/lint: `poetry run ruff check .` y `poetry run ruff format --check .`
  (línea máx. 90, target py312).
- Tipado: `poetry run mypy --config-file=.code_quality/mypy.ini cedenar_anomalies`
- Análisis estático: `poetry run pylint --rcfile=.code_quality/.pylintrc <archivos.py>`
- Seguridad: `poetry run bandit -c .code_quality/bandit.yaml -r cedenar_anomalies`
- Todo junto: `poetry run pre-commit run --all-files` (excluye `venv/` y notebooks `.ipynb`).

## Procedimiento

1. Determina los archivos `.py` cambiados (`git diff --name-only` y `git status`).
2. Ejecuta ruff (check + format --check), mypy, pylint y bandit sobre esos archivos
   (o sobre `cedenar_anomalies` si el cambio es amplio).
3. Revisa además, leyendo el diff:
   - Uso de `logging` en vez de `print` en código de librería.
   - Type hints en funciones nuevas/públicas.
   - Rutas vía `utils.config` / `utils.paths` (no rutas absolutas hardcodeadas).
   - Que no se filtren secretos de `.env`.

## Salida

- **Resumen**: ¿pasa pre-commit? (sí/no).
- **Bloqueantes**: errores de ruff/mypy/pylint/bandit con archivo:línea y fix sugerido.
- **Mejoras opcionales**: estilo/tipado/logging.
Aplica fixes triviales solo si el usuario lo pide; por defecto, diagnostica y recomienda.
```

- [ ] **Step 2: Verificar el frontmatter del agente**

Run: `head -6 .claude/agents/revisor-calidad.md`
Expected: muestra `---`, `name: revisor-calidad`, `description: ...`, `tools: Read, Grep, Glob, Bash`, `model: sonnet`, `---`.

- [ ] **Step 3: Commit**

```bash
git add .claude/agents/revisor-calidad.md
git commit -m "feat(claude): añadir subagente revisor-calidad"
```

---

## Tarea 6: Skill `ml-pipeline`

**Files:**
- Create: `.claude/skills/ml-pipeline/SKILL.md`

- [ ] **Step 1: Crear `.claude/skills/ml-pipeline/SKILL.md`**

```markdown
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
```

- [ ] **Step 2: Verificar el skill**

Run: `head -4 .claude/skills/ml-pipeline/SKILL.md`
Expected: muestra `---`, `name: ml-pipeline`, `description: ...`, `---`.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/ml-pipeline/SKILL.md
git commit -m "feat(claude): añadir skill ml-pipeline"
```

---

## Tarea 7: Skill `migraciones-alembic`

**Files:**
- Create: `.claude/skills/migraciones-alembic/SKILL.md`

- [ ] **Step 1: Crear `.claude/skills/migraciones-alembic/SKILL.md`**

```markdown
---
name: migraciones-alembic
description: Úsalo para crear, revisar o aplicar migraciones de base de datos con Alembic en cedenar_anomalies (Postgres, SQLAlchemy 2.x async asyncpg + sync psycopg2) y para trabajar con los repositorios SQL.
---

# Skill: Migraciones con Alembic

Guía para gestionar el esquema de Postgres en `cedenar_anomalies`.

## Contexto

- Config: `alembic.ini` (raíz) + `migrations/env.py`. Migraciones en `migrations/versions/`.
- Modelos ORM (origen del autogenerate): `cedenar_anomalies/infrastructure/database/models.py`
  (p. ej. `AnomaliaData`, tabla de inference, columnas de puntaje).
- Sesiones/engine: `infrastructure/database/session.py`, init en `infrastructure/database/init_db.py`.
- URLs de conexión: `utils/config.py` expone `DATABASE_URL` (async, `postgresql+asyncpg://`) y
  `SYNC_DATABASE_URL` (sync, `postgresql://`). Vienen de `.env` (NO la leas/expongas).
- Helper async: `cedenar_anomalies/utils/async_alembic.py`.
- Migraciones existentes (referencia de naming `YYYYMMDD_HHMM-<rev>_<slug>.py`):
  `add_anomalia_table`, `add_inference_model_table`, `add_puntaje_columns`.

## Crear una migración

1. Modifica/añade el modelo ORM en `infrastructure/database/models.py`.
2. Autogenera la revisión:
   ```bash
   poetry run alembic revision --autogenerate -m "add_<descripcion>"
   ```
3. **Revisa el archivo generado** en `migrations/versions/` antes de aplicarlo (Alembic no detecta
   todo: renombres, cambios de tipo sutiles, constraints). Edita `upgrade()`/`downgrade()` si hace falta.

## Aplicar / revertir

```bash
poetry run alembic current          # revisión actual
poetry run alembic history          # historial
poetry run alembic upgrade head     # aplicar pendientes
poetry run alembic downgrade -1     # revertir la última (¡con cuidado!)
```

## Reglas

- Confirma que Postgres esté arriba (`docker-compose.yml` define el servicio) y que `.env` tenga
  `DATABASE_URL` válido antes de migrar.
- `alembic downgrade base` está en la lista `ask` de settings.json: pide confirmación al usuario.
- Mantén async y sync coherentes: usa `DATABASE_URL` (async) y `SYNC_DATABASE_URL` cuando aplique.
- Commits con ámbito `db` y mensaje conventional, p. ej. `feat(db): add_<tabla>_table`.
```

- [ ] **Step 2: Verificar el skill**

Run: `head -4 .claude/skills/migraciones-alembic/SKILL.md`
Expected: muestra `---`, `name: migraciones-alembic`, `description: ...`, `---`.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/migraciones-alembic/SKILL.md
git commit -m "feat(claude): añadir skill migraciones-alembic"
```

---

## Tarea 8: Skill `datos-externos` (BigQuery / Google Sheets / ngrok)

**Files:**
- Create: `.claude/skills/datos-externos/SKILL.md`

- [ ] **Step 1: Crear `.claude/skills/datos-externos/SKILL.md`**

```markdown
---
name: datos-externos
description: Úsalo al enviar datos a BigQuery o Google Sheets, o al exponer datos a Looker mediante el túnel ngrok, en cedenar_anomalies.
---

# Skill: Integraciones de datos externos (BigQuery / Google Sheets / Looker)

Guía para las salidas de datos del proyecto hacia servicios de Google y Looker.

## Componentes

- BigQuery: `application/send_to_BQ_anomalia.py`, `application/send_to_BQ_inference.py`
  (usan `google-cloud-bigquery` / `pandas-gbq` / `google-auth`).
- Google Sheets: cargas vía `gspread` + `oauth2client` (scripts de `application/`).
- Carga a DB de anomalías: `application/load_to_anomalia_db.py`,
  `application/load_inference_to_db.py`.
- Túnel para Looker: `run_ngrok_for_looker.py` (raíz) +
  `infrastructure/adapters/ngrok_connector/ngrok_manager.py` +
  `domain/services/tunnel_service.py`.

## Credenciales

- Toda credencial (service account de Google, tokens, claves API) se lee desde `.env` vía
  `utils/config.py`. **NUNCA** leas, imprimas ni commitees `.env` ni archivos `*.json` de
  credenciales (los `*.json` están ignorados por `.gitignore`).
- Verifica que las variables necesarias existan antes de ejecutar; si faltan, pídeselas al usuario
  (no las inventes).

## Operaciones típicas

```bash
# Enviar inferencia a BigQuery (también lo hace el pipeline de inferencia al final)
poetry run python cedenar_anomalies/application/send_to_BQ_inference.py

# Enviar anomalías a BigQuery
poetry run python cedenar_anomalies/application/send_to_BQ_anomalia.py

# Levantar túnel ngrok para que Looker consuma la DB
poetry run python run_ngrok_for_looker.py
```

## Reglas

- Respeta la arquitectura: la lógica de conexión externa vive en `infrastructure/adapters/`
  detrás de un puerto del dominio; `application/` solo orquesta.
- El túnel ngrok expone un puerto local: confírmalo con el usuario antes de levantarlo y ciérralo
  al terminar.
- Commits con ámbito `bq`, `gsheets` o `etl` según corresponda.
```

- [ ] **Step 2: Verificar el skill**

Run: `head -4 .claude/skills/datos-externos/SKILL.md`
Expected: muestra `---`, `name: datos-externos`, `description: ...`, `---`.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/datos-externos/SKILL.md
git commit -m "feat(claude): añadir skill datos-externos"
```

---

## Tarea 9: Plantillas SDD y README de specs

**Files:**
- Create: `.claude/specs/templates/spec-template.md`
- Create: `.claude/specs/templates/plan-template.md`
- Create: `.claude/specs/templates/tasks-template.md`
- Create: `.claude/specs/README.md`

- [ ] **Step 1: Crear `.claude/specs/templates/spec-template.md`**

```markdown
# Especificación: <NOMBRE_FEATURE>

- **ID/slug:** <YYYY-MM-DD-slug>
- **Autor:** <nombre>
- **Estado:** borrador | aprobada | implementada
- **Capa(s) afectada(s):** domain / application / infrastructure / utils / migrations / models

## 1. Problema / Motivación
¿Qué necesidad de negocio (CEDENAR / anomalías) resuelve? ¿Por qué ahora?

## 2. Objetivo
Una frase: qué debe ser cierto cuando esté terminado.

## 3. Alcance
- **Incluye:** ...
- **No incluye (out of scope):** ...

## 4. Requisitos funcionales
- RF1: ...
- RF2: ...

## 5. Requisitos no funcionales / restricciones
- Arquitectura hexagonal (puertos + adaptadores).
- Estilo Python del repo (ruff/mypy/pylint/bandit, español, logging).
- Datos: usar `utils.config`/`utils.paths`; no commitear datos ni `.env`.

## 6. Datos y modelos implicados
- Datasets (`data/...`), modelos (`models/*.pkl`), tablas/migraciones, integraciones (BQ/GSheets).

## 7. Criterios de aceptación
- [ ] AC1: ...
- [ ] AC2: ...

## 8. Riesgos y preguntas abiertas
- ...
```

- [ ] **Step 2: Crear `.claude/specs/templates/plan-template.md`**

```markdown
# Plan: <NOMBRE_FEATURE>

> Derivado de `.claude/specs/<slug>/spec.md`. Ejecutar con superpowers:executing-plans.

**Goal:** <una frase>
**Capas afectadas:** <domain/application/infrastructure/...>

## Mapa de archivos
- Crear: `cedenar_anomalies/.../<archivo>.py` — responsabilidad
- Modificar: `cedenar_anomalies/.../<archivo>.py:<líneas>` — qué cambia

## Tareas (bite-sized)

### Tarea 1: <componente>
**Files:** Crear/Modificar: `...`
- [ ] Paso 1: <acción> (código completo si aplica)
- [ ] Paso 2: Verificar: `poetry run ...` → salida esperada
- [ ] Paso 3: Commit `feat(<ámbito>): ...`

### Tarea 2: <componente>
...

## Verificación final
- [ ] `poetry run pre-commit run --all-files` pasa
- [ ] Subagente `arquitecto-hexagonal`: CUMPLE
- [ ] Subagente `revisor-calidad`: sin bloqueantes
- [ ] Criterios de aceptación de la spec marcados
```

- [ ] **Step 3: Crear `.claude/specs/templates/tasks-template.md`**

```markdown
# Tareas ejecutables: <NOMBRE_FEATURE>

> Lista plana de tareas atómicas derivadas del plan. Cada una es commiteable por separado.

- [ ] T1 — <acción concreta> · archivos: `...` · verificación: `poetry run ...` · commit: `<tipo>(<ámbito>): ...`
- [ ] T2 — ...
- [ ] T3 — ...

## Definición de "hecho" (Definition of Done)
- [ ] pre-commit pasa (ruff, pylint, bandit, mypy).
- [ ] Respeta arquitectura hexagonal.
- [ ] Criterios de aceptación de la spec cumplidos.
- [ ] Commits con Conventional Commits.
```

- [ ] **Step 4: Crear `.claude/specs/README.md`**

```markdown
# Spec-Driven Development (SDD) en cedenar_anomalies

Flujo para convertir una idea en código de forma trazable, montado sobre **superpowers**.

## Fases

1. **Brainstorm** → usa el skill `superpowers:brainstorming` para explorar intención y diseño.
2. **Spec** (`/sdd-spec <slug>`) → crea `.claude/specs/<slug>/spec.md` desde
   `templates/spec-template.md`. Define problema, alcance, requisitos y criterios de aceptación.
3. **Plan** (`/sdd-plan <slug>`) → crea `.claude/specs/<slug>/plan.md` desde
   `templates/plan-template.md` (idealmente vía `superpowers:writing-plans`). Mapa de archivos +
   tareas bite-sized con verificación y commits.
4. **Tasks** (`/sdd-tasks <slug>`) → crea `.claude/specs/<slug>/tasks.md` desde
   `templates/tasks-template.md`: lista plana de tareas atómicas.
5. **Ejecución** → implementa con `superpowers:executing-plans` o
   `superpowers:subagent-driven-development`. Valida con los subagentes `arquitecto-hexagonal` y
   `revisor-calidad`, y cierra con `superpowers:verification-before-completion`.

## Estructura por feature

```
.claude/specs/
├── README.md
├── templates/
│   ├── spec-template.md
│   ├── plan-template.md
│   └── tasks-template.md
└── <YYYY-MM-DD-slug>/
    ├── spec.md
    ├── plan.md
    └── tasks.md
```

## Reglas

- Una spec por cambio no trivial. Aprueba la spec antes de planear.
- El plan no introduce decisiones de producto nuevas: deriva de la spec.
- Cada tarea termina en un commit Conventional. `git push` solo a petición del usuario.
- Los planes "grandes" generados por `superpowers:writing-plans` se guardan en
  `docs/superpowers/plans/`; los specs ligeros del repo viven en `.claude/specs/<slug>/`.
```

- [ ] **Step 5: Verificar las plantillas y el README**

Run: `find .claude/specs -type f | sort`
Expected:
```
.claude/specs/README.md
.claude/specs/templates/plan-template.md
.claude/specs/templates/spec-template.md
.claude/specs/templates/tasks-template.md
```

- [ ] **Step 6: Commit**

```bash
git add .claude/specs/
git commit -m "docs(claude): añadir plantillas y README del flujo SDD"
```

---

## Tarea 10: Slash-commands SDD y skill orquestador `sdd-flujo`

**Files:**
- Create: `.claude/commands/sdd-spec.md`
- Create: `.claude/commands/sdd-plan.md`
- Create: `.claude/commands/sdd-tasks.md`
- Create: `.claude/skills/sdd-flujo/SKILL.md`

- [ ] **Step 1: Crear `.claude/commands/sdd-spec.md`**

```markdown
---
description: Crea una especificación SDD nueva en .claude/specs/<slug>/spec.md a partir de la plantilla.
argument-hint: <slug-corto-de-la-feature>
---

Crea una especificación Spec-Driven Development para la feature: **$ARGUMENTS**.

Pasos:
1. Si la idea no está clara, usa primero el skill `superpowers:brainstorming` para explorar
   intención, requisitos y diseño con el usuario.
2. Crea la carpeta `.claude/specs/<slug>/` usando como `<slug>` la fecha de hoy + un nombre corto
   en kebab-case basado en `$ARGUMENTS` (ej. `2026-06-03-deteccion-region-pacifico`).
3. Copia la plantilla `.claude/specs/templates/spec-template.md` a
   `.claude/specs/<slug>/spec.md` y RELLÉNALA por completo (sin placeholders): problema, objetivo,
   alcance, requisitos funcionales y no funcionales, datos/modelos implicados, criterios de
   aceptación y riesgos. Respeta las reglas del repo (arquitectura hexagonal, estilo Python, datos).
4. Muestra al usuario un resumen de la spec y pide aprobación antes de pasar al plan (`/sdd-plan`).
```

- [ ] **Step 2: Crear `.claude/commands/sdd-plan.md`**

```markdown
---
description: Deriva el plan de implementación desde una spec aprobada (.claude/specs/<slug>/plan.md).
argument-hint: <slug-de-la-spec>
---

Genera el plan de implementación para la spec: **$ARGUMENTS**.

Pasos:
1. Lee `.claude/specs/$ARGUMENTS/spec.md`. Si no existe, lístame las specs disponibles en
   `.claude/specs/` y detente.
2. Usa el skill `superpowers:writing-plans` para producir un plan con tareas bite-sized (TDD/DRY/
   YAGNI, commits frecuentes), partiendo de `.claude/specs/templates/plan-template.md`.
3. Guarda el resultado en `.claude/specs/$ARGUMENTS/plan.md`. Asegúrate de:
   - Incluir un mapa de archivos exacto (rutas reales del paquete `cedenar_anomalies/`).
   - Que cada tarea tenga verificación (`poetry run ...`) y un commit Conventional.
   - Incluir verificación final con los subagentes `arquitecto-hexagonal` y `revisor-calidad`.
4. No introduzcas decisiones de producto nuevas: todo debe derivar de la spec.
```

- [ ] **Step 3: Crear `.claude/commands/sdd-tasks.md`**

```markdown
---
description: Deriva la lista plana de tareas ejecutables desde el plan (.claude/specs/<slug>/tasks.md).
argument-hint: <slug-de-la-spec>
---

Genera la lista de tareas ejecutables para: **$ARGUMENTS**.

Pasos:
1. Lee `.claude/specs/$ARGUMENTS/plan.md`. Si no existe, dime que primero hay que correr
   `/sdd-plan $ARGUMENTS` y detente.
2. A partir del plan, crea `.claude/specs/$ARGUMENTS/tasks.md` usando
   `.claude/specs/templates/tasks-template.md`: una lista plana de tareas atómicas, cada una con
   archivos afectados, comando de verificación y mensaje de commit Conventional.
3. Incluye la "Definición de hecho" (pre-commit, arquitectura, criterios de aceptación, commits).
4. Ofrece ejecutar las tareas con `superpowers:executing-plans` o
   `superpowers:subagent-driven-development`.
```

- [ ] **Step 4: Crear `.claude/skills/sdd-flujo/SKILL.md`**

```markdown
---
name: sdd-flujo
description: Úsalo cuando el usuario quiera abordar un cambio no trivial siguiendo Spec-Driven Development (spec → plan → tareas → ejecución) en cedenar_anomalies, orquestando los skills de superpowers.
---

# Skill: Flujo Spec-Driven Development (SDD)

Orquesta el desarrollo dirigido por especificaciones en `cedenar_anomalies`, reutilizando
superpowers. Detalle completo en `.claude/specs/README.md`.

## Cuándo usarlo

Cualquier feature, refactor o cambio de pipeline no trivial. Para fixes triviales de una línea,
puedes saltar el flujo.

## Pasos

1. **Brainstorm** — invoca `superpowers:brainstorming` para alinear intención y diseño.
2. **Spec** — ejecuta `/sdd-spec <slug>`: crea y rellena `.claude/specs/<slug>/spec.md`. Pide
   aprobación al usuario.
3. **Plan** — ejecuta `/sdd-plan <slug>` (usa `superpowers:writing-plans`): crea
   `.claude/specs/<slug>/plan.md` con tareas bite-sized.
4. **Tasks** — ejecuta `/sdd-tasks <slug>`: crea `.claude/specs/<slug>/tasks.md`.
5. **Ejecución** — implementa con `superpowers:executing-plans` o
   `superpowers:subagent-driven-development`.
6. **Verificación** — valida con los subagentes `arquitecto-hexagonal` y `revisor-calidad`, y
   cierra con `superpowers:verification-before-completion` antes de declarar terminado.

## Reglas

- No saltes de la idea al código sin spec aprobada en cambios no triviales.
- Cada tarea cierra con un commit Conventional; `git push` solo a petición del usuario.
- Respeta siempre las reglas de `.claude/rules/` (arquitectura, estilo, datos, git).
```

- [ ] **Step 5: Verificar commands y skill SDD**

Run:
```bash
ls -1 .claude/commands/ && echo "---" && head -4 .claude/skills/sdd-flujo/SKILL.md
```
Expected:
```
sdd-plan.md
sdd-spec.md
sdd-tasks.md
---
---
name: sdd-flujo
description: Úsalo cuando el usuario quiera abordar un cambio no trivial...
---
```

- [ ] **Step 6: Commit**

```bash
git add .claude/commands/ .claude/skills/sdd-flujo/
git commit -m "feat(claude): añadir comandos SDD y skill orquestador sdd-flujo"
```

---

## Tarea 11: Verificación integral y cierre

**Files:**
- (sin nuevos archivos; verificación de todo lo anterior)

- [ ] **Step 1: Verificar inventario completo de `.claude` + CLAUDE.md**

Run: `find .claude -type f | sort && echo "--- raiz ---" && ls CLAUDE.md`
Expected:
```
.claude/agents/arquitecto-hexagonal.md
.claude/agents/revisor-calidad.md
.claude/commands/sdd-plan.md
.claude/commands/sdd-spec.md
.claude/commands/sdd-tasks.md
.claude/rules/arquitectura-hexagonal.md
.claude/rules/datos-y-modelos.md
.claude/rules/estilo-python.md
.claude/rules/git-y-commits.md
.claude/settings.json
.claude/skills/datos-externos/SKILL.md
.claude/skills/migraciones-alembic/SKILL.md
.claude/skills/ml-pipeline/SKILL.md
.claude/skills/sdd-flujo/SKILL.md
.claude/specs/README.md
.claude/specs/templates/plan-template.md
.claude/specs/templates/spec-template.md
.claude/specs/templates/tasks-template.md
--- raiz ---
CLAUDE.md
```

- [ ] **Step 2: Validar JSON de settings y frontmatter de todos los `.md` con frontmatter**

Run:
```bash
python -c "import json; json.load(open('.claude/settings.json')); print('settings.json OK')"
for f in .claude/agents/*.md .claude/skills/*/SKILL.md .claude/commands/*.md; do
  head -1 "$f" | grep -q '^---$' && echo "frontmatter OK: $f" || echo "FALTA frontmatter: $f";
done
```
Expected: `settings.json OK` seguido de una línea `frontmatter OK: ...` por cada archivo (ninguna línea `FALTA`).

- [ ] **Step 3: Confirmar que CLAUDE.md referencia archivos de reglas existentes**

Run:
```bash
grep -o '@.claude/rules/[a-z-]*\.md' CLAUDE.md | sed 's/^@//' | while read p; do
  test -f "$p" && echo "OK import: $p" || echo "ROTO import: $p";
done
```
Expected: cuatro líneas `OK import: .claude/rules/...` (ninguna `ROTO`).

- [ ] **Step 4: Verificar que `git status` no dejó archivos sin commitear de `.claude`/`CLAUDE.md`**

Run: `git status --porcelain CLAUDE.md .claude`
Expected: sin salida (todo commiteado).

- [ ] **Step 5: Commit final del plan en docs (si no estaba)**

```bash
git add docs/superpowers/plans/2026-06-03-configuracion-claude-repo.md
git commit -m "docs(claude): añadir plan de configuración de .claude"
```

---

## Auto-revisión (checklist del autor)

**1. Cobertura del spec/argumentos** ("instala y crea agentes, skills, rules, y sdd"):
- ✅ **rules**: Tarea 2 (4 reglas) + importadas en CLAUDE.md (Tarea 3).
- ✅ **agentes**: Tarea 4 (`arquitecto-hexagonal`) y Tarea 5 (`revisor-calidad`) — cubre el "guardián de arquitectura hexagonal" pedido.
- ✅ **skills**: Tareas 6–8 (`ml-pipeline`, `migraciones-alembic`, `datos-externos`) — los 3 dominios elegidos + Tarea 10 (`sdd-flujo`).
- ✅ **SDD propio sobre superpowers**: Tareas 9 y 10 (plantillas, README, 3 comandos, skill orquestador que invoca brainstorming/writing-plans/executing-plans).
- ✅ **"configurar .claude para manejar el repo"**: Tarea 1 (`settings.json` con permisos) + CLAUDE.md (Tarea 3).
- ✅ **Idioma español**: todo el contenido en español.
- ✅ **Sin testing**: no se añade pytest (según decisión del usuario).

**2. Escaneo de placeholders:** No hay "TBD/TODO/implementar luego". Las únicas llaves `<...>` están dentro de **plantillas** (su propósito es ser rellenadas) y en comandos como `$ARGUMENTS` (variable real de slash-commands). Correcto.

**3. Consistencia de nombres:** Nombres de agentes (`arquitecto-hexagonal`, `revisor-calidad`) y skills (`ml-pipeline`, `migraciones-alembic`, `datos-externos`, `sdd-flujo`) coinciden entre frontmatter, CLAUDE.md, comandos y verificaciones. Rutas (`.claude/...`) consistentes en todas las tareas. Slugs SDD coherentes (`<YYYY-MM-DD-slug>`).
```
