"""Build, stage, and execute a versioned Airflow DAG profile."""

import json
import subprocess
from os import environ
from pathlib import Path
from typing import Annotated

import httpx
import typer

from pilot_kit import airflow_api, profiles

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


@app.command()
def build(
    case: profiles.Profile,
    output: Path,
    mount: Annotated[str, typer.Option(help="Mount path seen by Airflow.")] = (
        "/opt/airflow/pilot-profile"
    ),
    container: Annotated[str | None, typer.Option(help="Running mixed-image container.")] = None,
    dag_file: Annotated[Path | None, typer.Option(help="Pinned baseline DAG source.")] = None,
) -> None:
    """Export DAG files, runtime dependencies, and Airflow settings."""
    try:
        manifest = profiles.build(case, output, mount, container, baseline_source=dag_file)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from error
    typer.echo(json.dumps(manifest, indent=2))


@app.command("stage-kubernetes")
def stage_kubernetes(
    directory: Path,
    context: Annotated[str, typer.Option()],
    namespace: Annotated[str, typer.Option()],
    pod: Annotated[str, typer.Option()],
    container: Annotated[str, typer.Option()],
    destination: Annotated[str, typer.Option()],
) -> None:
    """Stage a complete profile to a new directory on an existing shared volume."""
    try:
        profiles.stage_to_kubernetes(directory, context, namespace, pod, container, destination)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from error
    typer.echo(destination)


@app.command()
def settings(
    directory: Path,
    existing: Annotated[Path | None, typer.Option(help="Existing Airflow config in JSON.")] = None,
) -> None:
    """Print merged Airflow config, preserving existing bundle and queue definitions."""
    try:
        current = json.loads(existing.read_text()) if existing else {}
        result = profiles.merge_settings(directory, current)
    except (OSError, ValueError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(1) from error
    typer.echo(json.dumps(result, indent=2))


@app.command()
def run(
    directory: Path,
    api_url: Annotated[str, typer.Option(help="Airflow base URL.")],
    run_id: Annotated[str, typer.Option(help="Stable Airflow run ID.")],
    submit: Annotated[bool, typer.Option(help="Submit once if the run ID is new.")] = False,
    timeout: Annotated[int, typer.Option(min=1)] = 600,
    items: Annotated[int, typer.Option(min=1, max=10_000_000)] = airflow_api.DEFAULT_ITEMS,
    shards: Annotated[int, typer.Option(min=1, max=64)] = airflow_api.DEFAULT_SHARDS,
) -> None:
    """Read an existing run or explicitly submit a single baseline or mixed DAG run."""
    token = environ.get("AIRFLOW_PROFILE_TOKEN")
    if not token:
        typer.echo("Set AIRFLOW_PROFILE_TOKEN in the process environment.", err=True)
        raise typer.Exit(1)
    try:
        manifest = profiles.load(directory)
        with airflow_api.client_for(api_url, token) as client:
            result = profiles.verify_run(
                client,
                profiles.Profile(manifest["profile"]),
                run_id,
                submit=submit,
                timeout=timeout,
                items=items,
                shards=shards,
            )
    except httpx.HTTPStatusError as error:
        typer.echo(f"Airflow API returned HTTP {error.response.status_code} for {run_id}", err=True)
        raise typer.Exit(1) from error
    except (httpx.RequestError, httpx.InvalidURL, OSError, ValueError, TimeoutError) as error:
        detail = str(error) if isinstance(error, ValueError) else type(error).__name__
        typer.echo(f"{run_id}: {detail}", err=True)
        raise typer.Exit(1) from error
    typer.echo(json.dumps(result))
    if result["matches"] is not True:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
