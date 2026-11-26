from datetime import datetime
from os import environ
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Literal, TypedDict
from urllib.parse import quote

import httpx
from dotenv import dotenv_values

DAG_ID = "pilot_portability"
DEFAULT_ITEMS = 10000
DEFAULT_SHARDS = 1
TARGETS = ("local", "docker", "kind", "kubernetes")
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
    return client_for(address, token)


def client_for(address: str, token: str) -> httpx.Client:
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
        raise ValueError("Use HTTPS or loopback HTTP with no credentials embedded in the URL.")
    return httpx.Client(
        base_url=f"{str(url).rstrip('/')}/api/v2/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15.0,
    )


def request(client: httpx.Client, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    response.raise_for_status()
    return response.json()


def trigger(
    client: httpx.Client, run_id: str, conf: dict[str, int], *, dag_id: str = DAG_ID
) -> None:
    request(
        client,
        "POST",
        f"dags/{quote(dag_id, safe='')}/dagRuns",
        json={
            "dag_run_id": run_id,
            "logical_date": None,
            "conf": conf,
        },
    )


def run_path(dag_id: str, run_id: str) -> str:
    return f"dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}"


def get_run(
    client: httpx.Client,
    run_id: str,
    *,
    dag_id: str = DAG_ID,
    timeout: float = 15.0,
) -> dict[str, Any]:
    return request(client, "GET", run_path(dag_id, run_id), timeout=timeout)


def get_return_value(client: httpx.Client, dag_id: str, run_id: str, task_id: str) -> Any:
    path = f"{run_path(dag_id, run_id)}/taskInstances/{quote(task_id, safe='')}"
    return request(client, "GET", f"{path}/xcomEntries/return_value")["value"]


def wait_for_run(
    client: httpx.Client, run_id: str, timeout: float, interval: float, *, dag_id: str = DAG_ID
) -> dict[str, Any]:
    deadline = monotonic() + timeout
    while (remaining := deadline - monotonic()) > 0:
        run = get_run(client, run_id, dag_id=dag_id, timeout=min(15.0, remaining))
        if run["state"] in TERMINAL_STATES:
            return run
        sleep(max(0.0, min(interval, deadline - monotonic())))
    raise TimeoutError(
        f"Polling deadline reached for {run_id}. The Airflow run may still be active."
    )


def collect(client: httpx.Client, target: str, run: dict[str, Any]) -> RunResult:
    run_id = run["dag_run_id"]
    state: Literal["queued", "running", "success", "failed"] = run["state"]
    conf = run.get("conf") or {}
    items = conf.get("items", DEFAULT_ITEMS)
    shards = conf.get("shards", DEFAULT_SHARDS)
    expected = str((items - 1) * items * (2 * items - 1) // 6)
    total = None
    if state == "success":
        total = get_return_value(client, DAG_ID, run_id, "total")
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
