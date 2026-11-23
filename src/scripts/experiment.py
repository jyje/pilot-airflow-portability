"""Run the same Airflow workload across a matrix of environments and parameters."""

import json
from enum import StrEnum
from itertools import product
from typing import Annotated
from uuid import uuid4

import httpx
import typer

from pilot_kit import airflow_api

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


class Environment(StrEnum):
    LOCAL = "local"
    KIND = "kind"
    KUBERNETES = "kubernetes"


@app.callback()
def main() -> None:
    """Airflow portability experiments."""


@app.command()
def run(
    target: Annotated[list[Environment], typer.Option(help="Repeat to select environments.")],
    items: Annotated[list[int] | None, typer.Option(min=1, max=10_000_000)] = None,
    shards: Annotated[list[int] | None, typer.Option(min=1, max=64)] = None,
    repeat: Annotated[int, typer.Option(min=1, max=100)] = 3,
    timeout: Annotated[int, typer.Option(min=1, help="Polling deadline per run in seconds.")] = 600,
    poll_interval: Annotated[float, typer.Option(min=0.1, max=60)] = 2.0,
) -> None:
    """Trigger the parameter matrix and emit one JSON record per attempted run."""
    group = uuid4().hex[:16]
    typer.echo(f"Experiment group: {group}", err=True)
    failed = False
    workloads = list(product(dict.fromkeys(items or [10000]), dict.fromkeys(shards or [4])))
    for environment in dict.fromkeys(target):
        try:
            client = airflow_api.connect(environment)
        except (ValueError, httpx.InvalidURL) as error:
            typer.echo(f"{environment}: {error}", err=True)
            failed = True
            continue
        with client:
            for (size, partitions), attempt in product(workloads, range(1, repeat + 1)):
                run_id = f"pilot-{group}-{size}-{partitions}-{attempt}"
                typer.echo(f"{environment}: {run_id}", err=True)
                try:
                    airflow_api.trigger(client, run_id, size, partitions)
                    completed = airflow_api.wait_for_run(client, run_id, timeout, poll_interval)
                    result = airflow_api.collect(client, environment, completed)
                    typer.echo(json.dumps(result))
                    failed |= result["state"] != "success" or result["matches"] is not True
                except (httpx.HTTPError, TimeoutError) as error:
                    detail = (
                        f"HTTP {error.response.status_code}"
                        if isinstance(error, httpx.HTTPStatusError)
                        else type(error).__name__
                    )
                    typer.echo(
                        json.dumps(
                            {
                                "environment": environment,
                                "run_id": run_id,
                                "state": "client_error",
                                "items": size,
                                "shards": partitions,
                                "error": detail,
                            }
                        )
                    )
                    failed = True
    if failed:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
