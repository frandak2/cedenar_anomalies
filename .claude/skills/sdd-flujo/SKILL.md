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
