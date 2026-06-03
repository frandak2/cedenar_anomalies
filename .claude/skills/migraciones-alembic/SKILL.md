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
