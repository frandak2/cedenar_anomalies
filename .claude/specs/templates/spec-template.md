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
