import json

import httpx
from typer.testing import CliRunner

from scripts import experiment


def test_matrix_and_mismatched_result(monkeypatch):
    triggered = []

    def connect(target: str) -> httpx.Client:
        runs = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                trigger = json.loads(request.content)
                triggered.append((target, trigger["conf"]))
                runs[trigger["dag_run_id"]] = trigger["conf"]
                return httpx.Response(200, json={})
            if request.url.path.endswith("return_value"):
                return httpx.Response(200, json={"value": "0"})
            run_id = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(
                200,
                json={
                    "dag_run_id": run_id,
                    "state": "success",
                    "conf": runs[run_id],
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
            "--target",
            "kind",
            "--items",
            "1",
            "--items",
            "2",
            "--shards",
            "2",
            "--repeat",
            "2",
        ],
    )
    assert result.exit_code == 1
    assert "Experiment group:" in result.stderr
    assert len(triggered) == 8
    results = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(results) == 8
    assert {entry["environment"] for entry in results} == {"local", "kind"}
    assert sum(entry["matches"] for entry in results) == 4
