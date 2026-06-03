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
