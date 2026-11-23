from collections.abc import Iterator
from datetime import datetime
from os import environ
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Literal, TypedDict
from urllib.parse import quote

import httpx
from dotenv import dotenv_values

DAG_ID = "pilot_portability"
TARGETS = ("local", "kind", "kubernetes")
TERMINAL_STATES = {"success", "failed"}
ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class RunResult(TypedDict):
    environment: str
    run_id: str
    state: str
    items: int
    shards: int
    duration_seconds: float | None
    expected_total: str
    total: str | None
    matches: bool | None


def connect(target: str) -> httpx.Client:
    if target not in TARGETS:
        raise ValueError(f"Unknown environment: {target}")
    settings = dotenv_values(ENV_FILE) | dict(environ)
    prefix = f"AIRFLOW_{target.upper()}"
    address = settings.get(f"{prefix}_URL")
    token = settings.get(f"{prefix}_TOKEN")
    if not address or not token:
        raise ValueError(f"Set {prefix}_URL and {prefix}_TOKEN in the environment or .env.")
    url = httpx.URL(address)
    if (
        not url.host
        or url.userinfo
        or url.query
        or url.fragment
        or (
            url.scheme != "https"
            and not (url.scheme == "http" and url.host in {"localhost", "127.0.0.1", "::1"})
        )
    ):
        raise ValueError(
            f"{prefix}_URL must use HTTPS or loopback HTTP without embedded credentials."
        )
    return httpx.Client(
        base_url=f"{str(url).rstrip('/')}/api/v2/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15.0,
    )


def request(client: httpx.Client, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    response.raise_for_status()
    return response.json()


def trigger(client: httpx.Client, run_id: str, items: int, shards: int) -> None:
    request(
        client,
        "POST",
        f"dags/{DAG_ID}/dagRuns",
        json={
            "dag_run_id": run_id,
            "logical_date": None,
            "conf": {"items": items, "shards": shards},
        },
    )


def wait_for_run(
    client: httpx.Client, run_id: str, timeout: float, interval: float
) -> dict[str, Any]:
    deadline = monotonic() + timeout
    path = f"dags/{DAG_ID}/dagRuns/{quote(run_id, safe='')}"
    while (remaining := deadline - monotonic()) > 0:
        run = request(client, "GET", path, timeout=min(15.0, remaining))
        if run["state"] in TERMINAL_STATES:
            return run
        sleep(max(0.0, min(interval, deadline - monotonic())))
    raise TimeoutError(
        f"Polling deadline reached for {run_id}; the Airflow run may still be active."
    )


def list_runs(client: httpx.Client, group: str) -> Iterator[dict[str, Any]]:
    prefix = f"pilot-{group}-"
    offset = 0
    while True:
        page = request(
            client,
            "GET",
            f"dags/{DAG_ID}/dagRuns",
            params={"run_id_pattern": prefix, "limit": 50, "offset": offset, "order_by": "id"},
        )
        runs = page["dag_runs"]
        yield from (run for run in runs if run["dag_run_id"].startswith(prefix))
        offset += len(runs)
        if not runs or offset >= page["total_entries"]:
            break


def collect(client: httpx.Client, target: str, run: dict[str, Any]) -> RunResult:
    run_id = run["dag_run_id"]
    state: Literal["queued", "running", "success", "failed"] = run["state"]
    items = run["conf"]["items"]
    shards = run["conf"]["shards"]
    expected = str((items - 1) * items * (2 * items - 1) // 6)
    total = None
    if state == "success":
        result = request(
            client,
            "GET",
            f"dags/{DAG_ID}/dagRuns/{quote(run_id, safe='')}/taskInstances/total"
            "/xcomEntries/return_value",
        )
        total = result["value"]
    start, end = run.get("start_date"), run.get("end_date")
    duration = (
        (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
        if start and end
        else None
    )
    return {
        "environment": target,
        "run_id": run_id,
        "state": state,
        "items": items,
        "shards": shards,
        "duration_seconds": duration,
        "expected_total": expected,
        "total": total,
        "matches": total == expected if state == "success" else None,
    }
