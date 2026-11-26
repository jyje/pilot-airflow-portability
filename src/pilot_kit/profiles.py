"""Prepare a deployable DAG profile and run it through an Airflow 3.4+ API."""

import hashlib
import json
import re
import shutil
import subprocess
import tarfile
from enum import StrEnum
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import quote

import httpx

from pilot_kit import airflow_api, workflows

SOURCE = Path(__file__).resolve().parents[1] / "dags"
IMAGE_DAGS = "/opt/airflow/pilot-dags"


class Profile(StrEnum):
    BASELINE = "baseline"
    MIXED = "mixed"


DAG_IDS = {
    Profile.BASELINE: airflow_api.DAG_ID,
    Profile.MIXED: workflows.DAG_IDS[workflows.Case.MIXED],
}


def build(
    profile: Profile,
    output: Path,
    mount: str,
    container: str | None = None,
    *,
    baseline_source: Path | None = None,
) -> dict[str, Any]:
    mount_path = PurePosixPath(mount)
    if not mount_path.is_absolute() or mount_path == PurePosixPath("/") or ".." in mount_path.parts:
        raise ValueError("The Airflow mount must be an absolute directory below /.")
    if profile is Profile.MIXED and not container:
        raise ValueError("The mixed profile requires a running container from the mixed image.")
    if profile is Profile.MIXED and baseline_source is not None:
        raise ValueError("A source override is supported only for the baseline DAG.")
    if output.exists():
        raise ValueError("The profile output already exists.")

    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".pilot-profile-", dir=output.parent) as scratch:
        stage = Path(scratch) / "profile"
        dags = stage / "dags"
        dags.mkdir(parents=True)
        bundle: dict[str, Any] = {
            "name": f"pilot-{profile}",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(mount_path / "dags")},
        }
        settings: dict[str, Any] = {"dag_processor": {"dag_bundle_config_list": [bundle]}}
        platform = None
        if profile is Profile.BASELINE:
            source = dags / "portability.py"
            shutil.copyfile(baseline_source or SOURCE / source.name, source)
        else:
            assert container
            platform = subprocess.check_output(
                ["docker", "exec", container, "uname", "-sm"], text=True
            ).strip()
            if not platform.startswith("Linux "):
                raise ValueError("The mixed profile requires Linux runtime artifacts.")
            runtime = stage / "runtime"
            runtime.mkdir()
            for original, destination in (
                (f"{IMAGE_DAGS}/chain.yaml", dags / "chain.yaml"),
                (f"{IMAGE_DAGS}/.airflowignore", dags / ".airflowignore"),
                (f"{IMAGE_DAGS}/handlers", dags / "handlers"),
                ("/usr/local/bin/node", runtime / "node"),
                ("/opt/java/openjdk", runtime / "openjdk"),
            ):
                subprocess.run(
                    ["docker", "cp", f"{container}:{original}", str(destination)], check=True
                )
            source = dags / "chain.yaml"
            bundle["importers"] = [
                {"classpath": "airflow.sdk.importers.yaml_importer.YamlDagImporter"}
            ]
            settings["sdk"] = {
                "coordinators": {
                    "pilot-node": {
                        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                        "kwargs": {"node_executable": str(mount_path / "runtime/node")},
                    },
                    "pilot-go": {
                        "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator"
                    },
                    "pilot-java": {
                        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
                        "kwargs": {"java_executable": str(mount_path / "runtime/openjdk/bin/java")},
                    },
                },
                "queue_to_coordinator": {
                    "typescript": "pilot-node",
                    "golang": "pilot-go",
                    "jvm": "pilot-java",
                },
            }

        manifest = {
            "profile": str(profile),
            "dag_id": DAG_IDS[profile],
            "minimum_airflow": "3.4.0",
            "runtime_platform": platform,
            "mount_path": str(mount_path),
            "dag_file": str(source.relative_to(stage)),
            "dag_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "settings": settings,
        }
        (stage / "profile.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(output)
    return manifest


def load(directory: Path) -> dict[str, Any]:
    manifest = json.loads((directory / "profile.json").read_text())
    profile = Profile(manifest["profile"])
    filename = "portability.py" if profile is Profile.BASELINE else "chain.yaml"
    source = directory / "dags" / filename
    if (
        manifest["dag_id"] != DAG_IDS[profile]
        or manifest["dag_file"] != str(source.relative_to(directory))
        or hashlib.sha256(source.read_bytes()).hexdigest() != manifest["dag_sha256"]
    ):
        raise ValueError("The DAG source does not match its profile manifest.")
    return manifest


def merge_settings(directory: Path, current: dict[str, Any]) -> dict[str, Any]:
    settings = load(directory)["settings"]
    result = json.loads(json.dumps(current))
    processor = result.setdefault("dag_processor", {})
    bundles = processor.setdefault(
        "dag_bundle_config_list",
        [
            {
                "name": "dags-folder",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {},
            }
        ],
    )
    for bundle in settings["dag_processor"]["dag_bundle_config_list"]:
        previous = next((value for value in bundles if value["name"] == bundle["name"]), None)
        if previous is None:
            bundles.append(bundle)
        elif previous != bundle:
            raise ValueError(f'The existing DAG bundle "{bundle["name"]}" has different settings.')
    for key, additions in settings.get("sdk", {}).items():
        mapping = result.setdefault("sdk", {}).setdefault(key, {})
        overlap = mapping.keys() & additions.keys()
        if any(mapping[name] != additions[name] for name in overlap):
            raise ValueError(f"The existing SDK {key} mapping conflicts with this profile.")
        mapping.update(additions)
    return result


def stage_to_kubernetes(
    directory: Path,
    context: str,
    namespace: str,
    pod: str,
    container: str,
    destination: str,
) -> None:
    load(directory)
    target = PurePosixPath(destination)
    if not target.is_absolute() or target.parent == PurePosixPath("/") or ".." in target.parts:
        raise ValueError("Use a new directory under the pod's shared writable volume.")
    # Publish the full profile at once. A configured DAG bundle never sees a partial upload.
    script = """
set -eu
dest=$1
tmp=$(mktemp -d "$(dirname "$dest")/.pilot-profile.XXXXXX")
trap 'rm -rf -- "$tmp"' EXIT
test ! -e "$dest"
tar -xf - -C "$tmp"
mv -Tn -- "$tmp" "$dest"
test ! -e "$tmp"
"""
    with TemporaryDirectory(prefix="pilot-upload-") as scratch:
        archive = Path(scratch) / "profile.tar"
        with tarfile.open(archive, "w") as stream:
            stream.add(directory, arcname=".")
        with archive.open("rb") as stream:
            subprocess.run(
                [
                    "kubectl",
                    "--context",
                    context,
                    "-n",
                    namespace,
                    "exec",
                    "-i",
                    pod,
                    "-c",
                    container,
                    "--",
                    "bash",
                    "-c",
                    script,
                    "pilot-stage",
                    str(target),
                ],
                stdin=stream,
                check=True,
            )


def verify_run(
    client: httpx.Client,
    profile: Profile,
    run_id: str,
    *,
    submit: bool,
    timeout: float,
    items: int = airflow_api.DEFAULT_ITEMS,
    shards: int = airflow_api.DEFAULT_SHARDS,
) -> dict[str, Any]:
    version = airflow_api.request(client, "GET", "version")["version"]
    match = re.match(r"^(\d+)\.(\d+)", version)
    if not match or tuple(map(int, match.groups())) < (3, 4):
        raise ValueError("The profile requires Airflow 3.4 or later.")
    dag_id = DAG_IDS[profile]
    conf = {"items": items, "shards": shards} if profile is Profile.BASELINE else {}
    if submit:
        dag = airflow_api.request(client, "GET", f"dags/{quote(dag_id, safe='')}")
        if dag["is_paused"] or dag.get("has_import_errors"):
            raise ValueError("The DAG must be active and free of import errors before submission.")
        response = client.get(airflow_api.run_path(dag_id, run_id))
        if response.status_code == 404:
            try:
                airflow_api.trigger(client, run_id, conf, dag_id=dag_id)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 409:
                    raise
        else:
            response.raise_for_status()
    completed = airflow_api.wait_for_run(client, run_id, timeout, 2, dag_id=dag_id)
    if submit and (completed.get("conf") or {}) != conf:
        raise ValueError("The existing run ID has different inputs.")
    if profile is Profile.MIXED:
        return workflows.verify(client, workflows.Case.MIXED, completed)
    return dict(airflow_api.collect(client, "profile", completed))
