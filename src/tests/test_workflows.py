import httpx
import pytest

from pilot_kit.workflows import MIXED_STAGES, Case, verify


@pytest.mark.parametrize(
    ("history", "missing_task", "matches"),
    [([15, 7, 0], False, True), ([15, 0], False, False), ([15, 7, 0], True, False)],
)
def test_verify_loop_requires_the_full_trajectory(history, missing_task, matches):
    tasks = [
        {"task_id": "load_backlog", "state": "success"},
        {"task_id": "summarize", "state": "success"},
        *[
            {"task_id": f"reconcile_batch.{name}", "region_index": iteration, "state": "success"}
            for iteration in (1, 2, 3)
            for name in ("reserve_batch", "apply_batch", "measure", "backlog_empty")
        ],
    ]
    if missing_task:
        tasks.pop()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path.endswith("return_value"):
            return httpx.Response(
                200, json={"value": {"remaining_history": history, "processed": 23}}
            )
        return httpx.Response(200, json={"task_instances": tasks})

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        outcome = verify(client, Case.LOOP, {"dag_run_id": "pilot-loop", "state": "success"})
    assert outcome["matches"] is matches


@pytest.mark.parametrize(
    ("state", "collect_state", "matches"),
    [("skipped", "success", True), ("success", "success", False), ("skipped", "skipped", False)],
)
def test_verify_typescript_checks_all_tasks(state, collect_state, matches):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path.endswith("return_value"):
            return httpx.Response(
                200, json={"value": {"sales": 240, "refunds": 20, "net": 220, "valid": True}}
            )
        return httpx.Response(
            200,
            json={
                "task_instances": [
                    {"task_id": "collect.sales", "state": collect_state},
                    {"task_id": "collect.refunds", "state": "success"},
                    {"task_id": "reconcile", "state": "success"},
                    {"task_id": "can_publish", "state": "success"},
                    {"task_id": "publish", "state": "success"},
                    {"task_id": "audit", "state": "success"},
                    {"task_id": "quarantine", "state": state},
                ]
            },
        )

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        outcome = verify(client, Case.TYPESCRIPT, {"dag_run_id": "pilot-ts", "state": "success"})
    assert outcome["matches"] is matches


@pytest.mark.parametrize("wrong_stage", [None, "go"])
def test_verify_mixed_checks_intermediate_values(wrong_stage):
    values = {
        stage: "-".join(MIXED_STAGES[: index + 1]) for index, stage in enumerate(MIXED_STAGES)
    }
    if wrong_stage:
        values[wrong_stage] = "incorrect"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("return_value"):
            stage = request.url.path.split("/")[-3]
            return httpx.Response(200, json={"value": values[stage]})
        return httpx.Response(
            200,
            json={"task_instances": [{"task_id": stage, "state": "success"} for stage in values]},
        )

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        outcome = verify(client, Case.MIXED, {"dag_run_id": "pilot-mixed", "state": "success"})
    assert outcome["matches"] is (wrong_stage is None)
