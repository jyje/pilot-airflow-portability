from collections.abc import Iterable
from datetime import UTC, datetime

from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="pilot_portability",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    params={
        "items": Param(10000, type="integer", minimum=1, maximum=10_000_000),
        "shards": Param(4, type="integer", minimum=1, maximum=64),
    },
)
def portability():
    @task
    def partition() -> list[dict[str, int]]:
        params = get_current_context()["params"]
        items, shards = params["items"], params["shards"]
        return [
            {"start": items * index // shards, "stop": items * (index + 1) // shards}
            for index in range(shards)
        ]

    @task
    def sum_squares(start: int, stop: int) -> int:
        return sum(value * value for value in range(start, stop))

    @task
    def total(partials: Iterable[int]) -> str:
        return str(sum(partials))

    total(sum_squares.expand_kwargs(partition()))


portability()
