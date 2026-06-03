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
  `cedenar_anomalies.utils.paths` (`data_dir()`, `models_dir()`, ... — funciones `*_dir` generadas
  dinámicamente en `paths.py`). NO hardcodees rutas absolutas.
- **Config/secretos**: lee variables con `os.getenv` vía `utils/config.py` (que hace
  `load_dotenv()`). NUNCA imprimas ni commitees el contenido de `.env`.

## Antes de dar por terminado un cambio de Python

Corre, y deja pasar, los hooks relevantes:
```bash
poetry run ruff check . && poetry run ruff format --check . && \
poetry run mypy --config-file=.code_quality/mypy.ini cedenar_anomalies
```
O delega la revisión completa al subagente `revisor-calidad`.
