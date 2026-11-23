# Agent instructions

Never use em dashes. Use hyphens or colons.

Read the [pilot-workflow skill](.claude/skills/pilot-workflow/SKILL.md) and
[PLAN.md](PLAN.md) before changing the pilot.

- Use Python 3.13 and run uv commands from `src/`. Run the
  [Python checks](.claude/skills/python-lint/SKILL.md) for Python changes.
- Keep the shared DAG in `src/dags/` and the Airflow REST client in
  `src/pilot_kit/`. Keep experiment entry points in `src/scripts/` and
  comparative notebooks in `src/notebooks/`.
- Keep deployment files for local Compose, kind, and multi-node Kubernetes in
  `deploy/`. Use the shared DAG image across environments.
- Write code and documentation in English by default. Update Korean README and
  guide twins alongside the English source. Use the
  [centered README skill](.claude/skills/centered-readme/SKILL.md).
- Keep credentials out of code, logs, notebook outputs, and commits.
- Follow the [commit skill](.claude/skills/git-commit-helper/SKILL.md) when
  creating commits with the user's approval.
