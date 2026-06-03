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
