from datetime import UTC, datetime

import httpx
import pytest

from pilot_kit import airflow_api


def test_run_contract():
    now = datetime.now(UTC).isoformat()
    run = {
        "dag_run_id": "pilot-test-4-2-1",
        "state": "success",
        "conf": {"items": 4, "shards": 2},
        "start_date": now,
        "end_date": now,
    }
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("return_value"):
            return httpx.Response(200, json={"value": "14"})
        return httpx.Response(200, json=run)

    with httpx.Client(
        base_url="https://airflow.example/airflow/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        airflow_api.trigger(client, run["dag_run_id"], 4, 2)
        completed = airflow_api.wait_for_run(client, run["dag_run_id"], 1, 0.1)
        result = airflow_api.collect(client, "local", completed)

    assert requests[0].method == "POST"
    assert requests[0].url.path == "/airflow/api/v2/dags/pilot_portability/dagRuns"
    assert b'"logical_date":null' in requests[0].content
    assert result["duration_seconds"] == 0
    assert result["matches"] is True


def test_list_filters_other_groups_and_paginates():
    offsets = []

    def handler(request: httpx.Request) -> httpx.Response:
        offsets.append(request.url.params["offset"])
        assert request.url.params["run_id_pattern"] == "pilot-matrix-"
        run_id = "pilot-matrix-4-2-1" if offsets[-1] == "0" else "other-pilot-matrix-4-2-2"
        return httpx.Response(
            200,
            json={
                "dag_runs": [{"dag_run_id": run_id}],
                "total_entries": 2,
            },
        )

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        assert list(airflow_api.list_runs(client, "matrix")) == [
            {"dag_run_id": "pilot-matrix-4-2-1"}
        ]

    assert offsets == ["0", "1"]


def test_failed_run_does_not_query_xcom():
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected request"))
    ) as client:
        result = airflow_api.collect(
            client,
            "kind",
            {
                "dag_run_id": "pilot-test-2-1-1",
                "state": "failed",
                "conf": {"items": 2, "shards": 1},
            },
        )
    assert result["matches"] is None
    assert result["total"] is None


def test_remote_http_is_rejected(monkeypatch):
    monkeypatch.setenv("AIRFLOW_KIND_URL", "http://airflow.example")
    monkeypatch.setenv("AIRFLOW_KIND_TOKEN", "test-token")
    with pytest.raises(ValueError, match="HTTPS or loopback"):
        airflow_api.connect("kind")
