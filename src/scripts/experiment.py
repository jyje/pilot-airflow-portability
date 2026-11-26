"""Run or inspect one Airflow portability experiment."""

import json
from enum import StrEnum
from typing import Annotated
from uuid import uuid4

import httpx
import typer

from pilot_kit import airflow_api, workflows

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


class Environment(StrEnum):
    LOCAL = "local"
    DOCKER = "docker"
    KIND = "kind"
    KUBERNETES = "kubernetes"


def show_error(error: Exception) -> None:
    detail = (
        f"HTTP {error.response.status_code}"
        if isinstance(error, httpx.HTTPStatusError)
        else type(error).__name__
    )
    typer.echo(detail, err=True)


@app.command()
def run(
    target: Annotated[Environment, typer.Option(help="Airflow environment to run once.")],
    items: Annotated[int, typer.Option(min=1, max=10_000_000)] = airflow_api.DEFAULT_ITEMS,
    shards: Annotated[int, typer.Option(min=1, max=64)] = airflow_api.DEFAULT_SHARDS,
    timeout: Annotated[int, typer.Option(min=1, help="Polling deadline in seconds.")] = 600,
    poll_interval: Annotated[float, typer.Option(min=0.1, max=60)] = 2.0,
) -> None:
    """Submit one DAG run and check its result."""
    run_id = f"pilot-{uuid4().hex}"
    typer.echo(f"{target}: {run_id}", err=True)
    try:
        with airflow_api.connect(target) as client:
            airflow_api.trigger(client, run_id, {"items": items, "shards": shards})
            completed = airflow_api.wait_for_run(client, run_id, timeout, poll_interval)
            result = airflow_api.collect(client, target, completed)
    except (httpx.HTTPError, httpx.InvalidURL, TimeoutError, ValueError) as error:
        show_error(error)
        raise typer.Exit(1) from error

    typer.echo(json.dumps(result))
    if result["matches"] is not True:
        raise typer.Exit(1)


@app.command()
def inspect(
    target: Annotated[Environment, typer.Option(help="Environment holding the existing run.")],
    run_id: Annotated[str, typer.Option(help="Exact DAG run ID to read.")],
) -> None:
    """Read and check an existing run without triggering the DAG."""
    try:
        with airflow_api.connect(target) as client:
            result = airflow_api.collect(client, target, airflow_api.get_run(client, run_id))
    except (httpx.HTTPError, httpx.InvalidURL, ValueError) as error:
        show_error(error)
        raise typer.Exit(1) from error

    typer.echo(json.dumps(result))
    if result["matches"] is not True:
        raise typer.Exit(1)


@app.command()
def workflow(
    case: Annotated[workflows.Case, typer.Argument(help="Workflow to verify.")],
    target: Annotated[Environment, typer.Option()] = Environment.LOCAL,
    run_id: Annotated[str | None, typer.Option(help="Inspect an existing run.")] = None,
) -> None:
    """Run or inspect one additional workflow."""
    dag_id = workflows.DAG_IDS[case]
    try:
        with airflow_api.connect(target) as client:
            if run_id is None:
                run_id = f"pilot-{case}-{uuid4().hex}"
                typer.echo(f"{target}: {run_id}", err=True)
                airflow_api.trigger(client, run_id, {}, dag_id=dag_id)
            completed = airflow_api.wait_for_run(client, run_id, 180, 1, dag_id=dag_id)
            result = workflows.verify(client, case, completed)
    except (httpx.HTTPError, httpx.InvalidURL, TimeoutError, ValueError) as error:
        show_error(error)
        raise typer.Exit(1) from error
    typer.echo(json.dumps(result))
    if not result["matches"]:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
