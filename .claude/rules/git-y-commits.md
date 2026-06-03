# Regla: Git y commits

- **Conventional Commits obligatorio** (lo valida el hook `conventional-pre-commit` en
  `commit-msg`). Formato: `<tipo>(<ámbito opcional>): <descripción>`.
  - Tipos: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `build`, `ci`, `perf`, `style`.
  - Ejemplos del repo: `feat: add puntaje columns`, `chore(claude): ...`.
- **Ámbitos sugeridos** para este repo: `claude`, `domain`, `application`, `infra`, `db`, `ml`,
  `etl`, `bq`, `gsheets`, `docs`.
- **pre-commit**: antes de commitear, los hooks corren ruff (`--fix`), ruff-format, pylint, bandit
  y mypy sobre archivos Python (excluye `venv/` y notebooks `.ipynb`). Si fallan, corrige y reintenta.
- **Ramas**: la rama principal es `main`; el trabajo activo va en `dev` o ramas `feature/<nombre>`.
  No commitees directo a `main`; abre PR.
- **`git push` requiere confirmación** (configurado en `ask` de settings.json). No hagas push sin
  que el usuario lo pida explícitamente.
- Cierra los mensajes de commit generados por Claude con la línea de coautoría correspondiente.
