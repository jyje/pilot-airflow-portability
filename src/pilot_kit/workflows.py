from enum import StrEnum
from typing import Any

import httpx

from pilot_kit import airflow_api


class Case(StrEnum):
    LOOP = "loop"
    TYPESCRIPT = "typescript"
    MIXED = "mixed"


DAG_IDS = {
    Case.LOOP: "pilot_reconcile",
    Case.TYPESCRIPT: "pilot_ts_settlement",
    Case.MIXED: "pilot_mixed_language_chain",
}
MIXED_STAGES = (
    "start",
    "python",
    "typescript",
    "javascript",
    "go",
    "java",
    "kotlin",
    "scala",
    "yaml",
)
LOOP_TASKS = ("reserve_batch", "apply_batch", "measure", "backlog_empty")
SETTLEMENT_STATES = {
    "collect.sales": "success",
    "collect.refunds": "success",
    "reconcile": "success",
    "can_publish": "success",
    "publish": "success",
    "quarantine": "skipped",
    "audit": "success",
}


def verify(client: httpx.Client, case: Case, run: dict[str, Any]) -> dict[str, Any]:
    dag_id = DAG_IDS[case]
    run_id, state = run["dag_run_id"], run["state"]
    path = f"{airflow_api.run_path(dag_id, run_id)}/taskInstances"
    tasks = airflow_api.request(client, "GET", path, params={"limit": 100})["task_instances"]
    outcome = {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": state,
        "task_states": [
            {
                "task_id": task["task_id"],
                "state": task["state"],
                "iteration": task.get("region_index"),
                "try_number": task.get("try_number"),
            }
            for task in tasks
        ],
        "result": None,
        "matches": False,
    }
    if state != "success":
        return outcome

    if case == Case.MIXED:
        values = {
            stage: airflow_api.get_return_value(client, dag_id, run_id, stage)
            for stage in MIXED_STAGES
        }
        outcome["result"] = values
        outcome["matches"] = (
            len(tasks) == len(MIXED_STAGES)
            and {task["task_id"]: task["state"] for task in tasks}
            == dict.fromkeys(MIXED_STAGES, "success")
            and values
            == {stage: "-".join(MIXED_STAGES[: i + 1]) for i, stage in enumerate(MIXED_STAGES)}
        )
        return outcome

    result_task = "summarize" if case == Case.LOOP else "audit"
    result = airflow_api.get_return_value(client, dag_id, run_id, result_task)
    outcome["result"] = result
    if case == Case.LOOP:
        expected = {("load_backlog", None), ("summarize", None)} | {
            (f"reconcile_batch.{name}", iteration)
            for iteration in range(1, 4)
            for name in LOOP_TASKS
        }
        successful = {
            (task["task_id"], task.get("region_index"))
            for task in tasks
            if task["state"] == "success"
        }
        outcome["matches"] = (
            result == {"remaining_history": [15, 7, 0], "processed": 23}
            and len(tasks) == len(expected)
            and successful == expected
        )
    else:
        states = {task["task_id"]: task["state"] for task in tasks}
        outcome["matches"] = (
            result == {"sales": 240, "refunds": 20, "net": 220, "valid": True}
            and len(tasks) == len(SETTLEMENT_STATES)
            and states == SETTLEMENT_STATES
        )
    return outcome
