import tarfile
from pathlib import Path

import httpx
import pytest

from pilot_kit import profiles


def test_baseline_profile_detects_a_changed_dag(tmp_path: Path) -> None:
    folder = tmp_path / "baseline"
    manifest = profiles.build(profiles.Profile.BASELINE, folder, "/opt/airflow/pilot-profile")

    assert profiles.load(folder) == manifest
    bundle = manifest["settings"]["dag_processor"]["dag_bundle_config_list"][0]
    assert bundle["kwargs"]["path"] == "/opt/airflow/pilot-profile/dags"

    (folder / "dags" / "portability.py").write_text("modified")
    with pytest.raises(ValueError, match="does not match"):
        profiles.load(folder)


def test_profile_settings_append_to_existing_bundle_list(tmp_path: Path) -> None:
    folder = tmp_path / "baseline"
    profiles.build(profiles.Profile.BASELINE, folder, "/opt/airflow/pilot-profile")
    main = {"name": "existing", "kwargs": {"path": "/opt/airflow/existing"}}
    current = {
        "dag_processor": {"dag_bundle_config_list": [main]},
        "core": {"load_examples": False},
    }
    merged = profiles.merge_settings(folder, current)

    assert merged["dag_processor"]["dag_bundle_config_list"][0] == main
    assert len(current["dag_processor"]["dag_bundle_config_list"]) == 1
    assert profiles.merge_settings(folder, merged) == merged

    merged["dag_processor"]["dag_bundle_config_list"][1]["kwargs"]["path"] = "/wrong"
    with pytest.raises(ValueError, match="different settings"):
        profiles.merge_settings(folder, merged)


def test_profile_upload_streams_a_complete_archive(tmp_path: Path, monkeypatch) -> None:
    folder = tmp_path / "baseline"
    profiles.build(profiles.Profile.BASELINE, folder, "/opt/airflow/pilot-profile")

    def check_exec(args, *, stdin, check):
        assert check
        assert args[:10] == [
            "kubectl",
            "--context",
            "test",
            "-n",
            "pilot",
            "exec",
            "-i",
            "stager",
            "-c",
            "main",
        ]
        assert args[-1] == "/shared/pilot-baseline"
        with tarfile.open(fileobj=stdin, mode="r:") as archive:
            assert "./dags/portability.py" in archive.getnames()
            assert "./profile.json" in archive.getnames()

    monkeypatch.setattr(profiles.subprocess, "run", check_exec)
    profiles.stage_to_kubernetes(
        folder, "test", "pilot", "stager", "main", "/shared/pilot-baseline"
    )


@pytest.mark.parametrize(
    ("exists", "submit", "posts"), [(False, True, 1), (True, True, 0), (True, False, 0)]
)
def test_baseline_profile_submits_only_a_new_run(exists: bool, submit: bool, posts: int) -> None:
    submitted = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal submitted
        path = request.url.path
        if request.method == "POST":
            submitted += 1
            assert path.endswith("/dagRuns")
            return httpx.Response(200, json={})
        if path.endswith("/version"):
            return httpx.Response(200, json={"version": "3.4.0"})
        if path.endswith("/pilot_portability"):
            return httpx.Response(200, json={"is_paused": False})
        if path.endswith("/return_value"):
            return httpx.Response(200, json={"value": "333283335000"})
        if not exists and not submitted:
            return httpx.Response(404)
        return httpx.Response(
            200,
            json={
                "dag_run_id": "pilot-fixed",
                "state": "success",
                "conf": {"items": 10000, "shards": 1},
            },
        )

    with httpx.Client(
        base_url="https://airflow.example/api/v2/", transport=httpx.MockTransport(handler)
    ) as client:
        result = profiles.verify_run(
            client, profiles.Profile.BASELINE, "pilot-fixed", submit=submit, timeout=2
        )
    assert result["matches"]
    assert submitted == posts
