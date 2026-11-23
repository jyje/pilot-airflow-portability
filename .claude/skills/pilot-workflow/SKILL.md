---
name: pilot-workflow
description: Plan, run, verify, and document the Airflow portability pilot using the Python pilot template conventions.
---

# Pilot workflow

Adapted from `jyje/template-pilot-ai-python`. Read `AGENTS.md`, `GOAL.md`, and
`PLAN.md` for scope and progress.

1. Plan measurable cases in `PLAN.md` with one scoped work item per commit.
2. Work in the Python 3.13 uv project in `src/`. Keep Airflow REST calls in
   `pilot_kit/`, runnable Typer experiments in `scripts/`, executed comparisons
   in `notebooks/`, and the shared DAG in `dags/`. Keep environment-specific
   deployment files in `deploy/`.
3. Verify in three tiers. Run the offline checks from the `python-lint` skill,
   run repeated CLI experiments against authorized Airflow targets, and execute
   the notebook using the same run group. Record measured results and open
   verification gaps in `PLAN.md`.
4. Write English first and update the Korean twins in the same change. Use
   `centered-readme` for both README headers. Keep API tokens in ignored
   `.env` or process variables and inspect notebook outputs before publishing.
5. Use `git-commit-helper` when a commit is requested.
