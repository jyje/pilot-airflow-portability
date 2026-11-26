<div align="center">

# jyje/pilot-airflow-portability

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/airflow-site/main/landing-pages/site/static/images/airflow-logo-dark.svg">
  <img width="220" src="https://raw.githubusercontent.com/apache/airflow-site/main/landing-pages/site/static/images/airflow-logo.svg" alt="Apache Airflow">
</picture>

🚀 A pilot project to verify Airflow DAG portability across local Docker and external Kubernetes.

[English](README-en.md) / [한국어](README-ko.md)

</div>

## Start here

- [Deployment and verification guide](docs/operations-en.md) for the environment setup, experiment input, and pass criteria.
- [E03 task Pod placement](docs/e03-node-placement-en.md).
- [Plan](PLAN-en.md) for remaining work and completion criteria.
- [Experiment script](src/scripts/experiment.py) and [comparison notebook](src/notebooks/01-compare-airflow.ipynb).

## Execution scenarios

| Scenario | Work environment | Executor | Operator (TaskFlow) | Path |
| --- | --- | --- | --- | --- |
| E01 | Local Docker, task subprocess | [`LocalExecutor`](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) | `PythonOperator(@task)` | [`deploy/local/run.sh`](deploy/local/run.sh) |
| E02 | Single Docker host, temporary container per task | `LocalExecutor` | [`DockerOperator(@task.docker)`](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) | [`deploy/local/run-docker-task.sh`](deploy/local/run-docker-task.sh) |
| E03 | Existing Airflow on external Kubernetes, Pod per task | [`KubernetesExecutor`](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html) | `PythonOperator(@task)` | [`src/scripts/experiment.py`](src/scripts/experiment.py) |

Operator examples describe the baseline. [kind with Helm](deploy/kind/) is an optional reproduction environment outside the verification matrix.

## Verification matrix

Run each of the four experiments once in each environment. A pass records the run ID, every expected task outcome, the result, and execution isolation.

| Experiment | Expected result | E01 | E02 | E03 |
| --- | --- | --- | --- | --- |
| Baseline | All three tasks succeed, total `333283335000` | Pass | Pass | Pass |
| Reconciliation loop | `23 → 15 → 7 → 0`, three iterations | Pass | Pass | Pass |
| TypeScript settlement | Net `220`, publish and audit succeed, quarantine skips | Pass | Pass | Pass after retry |
| Mixed-language chain | All nine tasks and XCom values match | Pass | Pass | Pass after retry |

All twelve experiment and environment combinations passed.

[Verification results and run evidence](docs/results-en.md)

[Executor and isolation guide](docs/executors-en.md)
