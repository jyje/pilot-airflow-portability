# Airflow executors in this pilot

[English](executors-en.md) / [한국어](executors-ko.md) / [Deployment and verification](operations-en.md)

The **executor** decides where a task instance runs once the Airflow scheduler
has determined that it is ready. DAG dependencies control when the task becomes
eligible. [Airflow 3.4 executor overview](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html)

## Deployment and task isolation scenarios

Compare two dimensions: where Airflow is deployed and what runs the workload
for each task. For Kubernetes, record the Pod as the execution unit since
the worker container runs inside that Pod.

| Scenario | Work environment | Executor | Operator (TaskFlow) | Path |
| --- | --- | --- | --- | --- |
| Local Docker, task subprocess | Single host | [`LocalExecutor`](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`deploy/local/run.sh`](../deploy/local/run.sh) |
| Local Docker, task container | Single host | `LocalExecutor` | [`DockerOperator(@task.docker)`](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) | [`deploy/local/run-docker-task.sh`](../deploy/local/run-docker-task.sh) |
| kind + Helm | Local, single-node Kubernetes | [`KubernetesExecutor`](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html) | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`deploy/kind/`](../deploy/kind/) |
| External Kubernetes, existing Airflow | External Kubernetes cluster | Instance configuration | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`src/scripts/experiment.py`](../src/scripts/experiment.py) |

The table uses operator names. In TaskFlow syntax, `@task` corresponds to `PythonOperator` and `@task.docker` corresponds to `DockerOperator`. The [baseline DAG](../src/dags/portability.py) uses `@task`.

The task-container scenario adds Docker container isolation on the same host.
Compare it with the local subprocess run using the same DAG stages, input, and
expected output. Record the run ID and the task container image and ID.
kind uses the Pod-per-task model configured in the [Helm values](../deploy/kubernetes/values.yaml). The external scenario uses the installed Airflow instance. Record its effective executor and task isolation from that deployment. See the [verification plan](../PLAN-en.md).

### Sequential and mixed-language tasks

In the proposed language chain, a task waits for its predecessor, reads its
XCom, and emits the next value, for example
`start → start-python → start-python-typescript`. The executor controls task
placement. Inside an Airflow worker, a language coordinator can start the
appropriate process, such as Node.js for TypeScript or JavaScript, a JVM for
Java, Kotlin, or Scala, or an executable for Go. The container or worker image
must supply the required runtimes and task bundles. XCom carries the string
between tasks across processes or Pods.
[Task execution architecture](https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html#task-execution-architecture) · [Language coordinator settings](https://airflow.apache.org/docs/apache-airflow/3.4.0/configurations-ref.html#sdk) · [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

### Temporary containers with Docker Engine

The [task-container launcher](../deploy/local/run-docker-task.sh) configures the baseline `sum_squares` task to use a Python-only workload image. It publishes the authenticated Airflow UI on loopback port 8086 and connects the Airflow worker to the Docker Engine. See [issue #2](https://github.com/jyje/pilot-airflow-portability/issues/2) for the observed container lifecycle.

Airflow supplies `DockerOperator` through the Docker provider. In a
`LocalExecutor` deployment, the executor runs the operator in an Airflow
worker process. The operator asks the configured Docker Engine to create a
container from the task's image, waits for its command to finish, and collects
the result. With `auto_remove="force"`, the container is removed when the
command exits. `@task.docker` provides the corresponding Python task
decorator. [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) · [Python Docker task decorator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/decorators/docker.html)

The Airflow worker needs access to that Engine through a Docker socket or API
endpoint. If it connects to the host Engine, the task container runs alongside
the Airflow container on that Engine. The pilot's local launcher currently
runs the Airflow container without exposing a Docker socket to it.
[Local launcher](../deploy/local/run.sh) · [Docker provider registry](https://airflow.apache.org/registry/providers/docker/)

## Other executor options

| Executor | Execution model |
| --- | --- |
| `CeleryExecutor` | Sends work through a message broker to long-running Celery workers. Each worker can run multiple task instances. [Celery executor](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html) |
| `EdgeExecutor` | Remote workers pull jobs from Airflow over HTTP(S). It supports workloads at remote sites or on edge devices. [Edge executor](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/edge_executor.html) |
| AWS provider executors | `AwsBatchExecutor` submits AWS Batch jobs, `AwsEcsExecutor` starts ECS tasks, `AwsLambdaExecutor` invokes Lambda, and `AwsEksExecutor` dispatches task Pods to EKS. [Community executor index](https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/executors.html) · [Amazon provider docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/executors/index.html) |
| Custom executors | An extension can implement `BaseExecutor` and be configured by its Python import path. [Writing an executor](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html#writing-your-own-executor) |

Airflow 3 supports listing multiple executors in `[core] executor`. The first is
the default, and a DAG or task can select another configured executor.
Airflow 3 uses this configuration in place of `CeleryKubernetesExecutor` and
`LocalKubernetesExecutor`. `LocalExecutor` replaces `SequentialExecutor` for
local development. A chain of task dependencies gives a run sequential order.
[Multiple executors](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html#using-multiple-executors-concurrently) · [Airflow 3 upgrade guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
