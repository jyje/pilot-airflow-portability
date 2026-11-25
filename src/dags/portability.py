from datetime import UTC, datetime
from os import environ

from airflow.sdk import Param, dag, task


@dag(
    dag_id="pilot_portability",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    params={
        "items": Param(10000, type="integer", minimum=1, maximum=10_000_000),
        "shards": Param(1, type="integer", minimum=1, maximum=64),
    },
)
def portability():
    def partition(items: int, shards: int) -> list[dict[str, int]]:
        return [
            {"start": items * index // shards, "stop": items * (index + 1) // shards}
            for index in range(shards)
        ]

    def sum_squares(start: int, stop: int) -> int:
        return sum(value * value for value in range(start, stop))

    def total(partials: list[int]) -> str:
        return str(sum(partials))

    args = ("{{ params['items'] }}", "{{ params['shards'] }}")
    if image := environ.get("PILOT_DOCKER_TASK_IMAGE"):
        docker_task = task.docker(
            image=image,
            docker_url="unix:///var/run/docker.sock",
            mount_tmp_dir=False,
            expect_airflow=False,
            auto_remove="force",
        )
        labels = {"org.jyje.airflow.scenario": "docker-task"}
        partitions = docker_task(partition).override(
            task_id="partition",
            labels={**labels, "org.jyje.airflow.task": "partition"},
        )(*args)
        partials = (
            docker_task(sum_squares)
            .override(
                task_id="sum_squares",
                labels={**labels, "org.jyje.airflow.task": "sum_squares"},
            )
            .expand_kwargs(partitions)
        )
        aggregate = docker_task(total).override(
            task_id="total",
            labels={**labels, "org.jyje.airflow.task": "total"},
        )("{{ ti.xcom_pull(task_ids='sum_squares', map_indexes=range(params.shards)) | list }}")
        partials >> aggregate
    else:
        task(total)(task(sum_squares).expand_kwargs(task(partition)(*args)))


portability()
