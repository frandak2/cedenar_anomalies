---
name: datos-externos
description: Úsalo al enviar datos a BigQuery o Google Sheets, o al exponer datos a Looker mediante el túnel ngrok, en cedenar_anomalies.
---

# Skill: Integraciones de datos externos (BigQuery / Google Sheets / Looker)

Guía para las salidas de datos del proyecto hacia servicios de Google y Looker.

## Componentes

- BigQuery: `application/send_to_BQ_anomalia.py`, `application/send_to_BQ_inference.py`
  (usan `google-cloud-bigquery` / `pandas-gbq` / `google-auth`).
- Google Sheets: dependencias `gspread` + `oauth2client` disponibles en el proyecto (aún SIN
  script dedicado en `application/`). Si creas uno, ponlo detrás de un adaptador en
  `infrastructure/adapters/`.
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
