# Spec-Driven Development (SDD) en cedenar_anomalies

Flujo para convertir una idea en código de forma trazable, montado sobre **superpowers**.

## Fases

1. **Brainstorm** → usa el skill `superpowers:brainstorming` para explorar intención y diseño.
2. **Spec** (`/sdd-spec <slug>`) → crea `.claude/specs/<slug>/spec.md` desde
   `templates/spec-template.md`. Define problema, alcance, requisitos y criterios de aceptación.
3. **Plan** (`/sdd-plan <slug>`) → crea `.claude/specs/<slug>/plan.md` desde
   `templates/plan-template.md` (idealmente vía `superpowers:writing-plans`). Mapa de archivos +
   tareas bite-sized con verificación y commits.
4. **Tasks** (`/sdd-tasks <slug>`) → crea `.claude/specs/<slug>/tasks.md` desde
   `templates/tasks-template.md`: lista plana de tareas atómicas.
5. **Ejecución** → implementa con `superpowers:executing-plans` o
   `superpowers:subagent-driven-development`. Valida con los subagentes `arquitecto-hexagonal` y
   `revisor-calidad`, y cierra con `superpowers:verification-before-completion`.

## Estructura por feature

```
.claude/specs/
├── README.md
├── templates/
│   ├── spec-template.md
│   ├── plan-template.md
│   └── tasks-template.md
└── <YYYY-MM-DD-slug>/
    ├── spec.md
    ├── plan.md
    └── tasks.md
```

## Reglas

- Una spec por cambio no trivial. Aprueba la spec antes de planear.
- El plan no introduce decisiones de producto nuevas: deriva de la spec.
- Cada tarea termina en un commit Conventional. `git push` solo a petición del usuario.
- Los planes "grandes" generados por `superpowers:writing-plans` se guardan en
  `docs/superpowers/plans/`; los specs ligeros del repo viven en `.claude/specs/<slug>/`.
