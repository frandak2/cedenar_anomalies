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
