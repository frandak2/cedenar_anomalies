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
