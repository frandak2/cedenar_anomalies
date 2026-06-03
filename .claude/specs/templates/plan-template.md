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
