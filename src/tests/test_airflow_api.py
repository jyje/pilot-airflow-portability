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
        airflow_api.trigger(client, run["dag_run_id"], {"items": 4, "shards": 2})
        completed = airflow_api.wait_for_run(client, run["dag_run_id"], 1, 0.1)
        result = airflow_api.collect(client, "local", completed)

    assert requests[0].method == "POST"
    assert requests[0].url.path == "/airflow/api/v2/dags/pilot_portability/dagRuns"
    assert b'"logical_date":null' in requests[0].content
    assert result["duration_seconds"] == 0
    assert result["matches"] is True


def test_get_run_preserves_special_characters_and_does_not_write():
    run_id = "manual__2026-11-23T12:00:00+00:00"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.raw_path.endswith(b"manual__2026-11-23T12%3A00%3A00%2B00%3A00")
        return httpx.Response(200, json={"dag_run_id": run_id})

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        assert airflow_api.get_run(client, run_id) == {"dag_run_id": run_id}


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
