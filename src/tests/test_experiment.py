import json

import httpx
import pytest
from typer.testing import CliRunner

from scripts import experiment


@pytest.mark.parametrize(("total", "expected_exit"), [("14", 0), ("0", 1)])
def test_run_submits_once(monkeypatch, total, expected_exit):
    submitted = []

    def connect(target: str) -> httpx.Client:
        assert target == "local"

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                submitted.append(json.loads(request.content))
                return httpx.Response(200, json={})
            if request.url.path.endswith("return_value"):
                return httpx.Response(200, json={"value": total})
            return httpx.Response(
                200,
                json={
                    "dag_run_id": submitted[0]["dag_run_id"],
                    "state": "success",
                    "conf": submitted[0]["conf"],
                },
            )

        return httpx.Client(
            base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
        )

    monkeypatch.setattr(experiment.airflow_api, "connect", connect)
    result = CliRunner().invoke(
        experiment.app,
        [
            "run",
            "--target",
            "local",
            "--items",
            "4",
            "--shards",
            "2",
        ],
    )
    assert result.exit_code == expected_exit
    assert len(submitted) == 1
    assert submitted[0]["conf"] == {"items": 4, "shards": 2}
    assert json.loads(result.stdout)["matches"] is (expected_exit == 0)


def test_inspect_uses_defaults_for_ui_run_and_never_submits(monkeypatch):
    run_id = "manual__2026-11-23T12:00:00+00:00"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path.endswith("return_value"):
            return httpx.Response(200, json={"value": "333283335000"})
        assert request.url.path.endswith(run_id)
        return httpx.Response(200, json={"dag_run_id": run_id, "state": "success", "conf": {}})

    monkeypatch.setattr(
        experiment.airflow_api,
        "connect",
        lambda _: httpx.Client(
            base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
        ),
    )
    result = CliRunner().invoke(
        experiment.app, ["inspect", "--target", "local", "--run-id", run_id]
    )
    assert result.exit_code == 0
    assert json.loads(result.stdout)["matches"] is True
