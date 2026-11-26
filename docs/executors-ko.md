# 이 파일럿의 Airflow 실행기

[English](executors-en.md) / [한국어](executors-ko.md) / [배포 및 검증](operations-ko.md)

Airflow 스케줄러가 태스크를 실행할 수 있다고 판단하면 **실행기(executor)**가
그 태스크 인스턴스를 어디에서 실행할지 결정합니다. 실행 가능 시점은 DAG에 정의된
의존성이 결정합니다. [Airflow 3.4 실행기 개요](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html)

## 배포 환경과 태스크 격리 방식

Airflow를 어디에 배포하는지와 각 태스크의 작업이 어떤 단위로 격리되는지를
비교합니다. Kubernetes에서는 워커 컨테이너를 담는 Pod를 실행 단위로 기록합니다.

| 시나리오 | 작업 환경 | Executor | Operator (TaskFlow) | 경로 |
| --- | --- | --- | --- | --- |
| 로컬 Docker, 태스크 프로세스 | 단일 호스트 | [`LocalExecutor`](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`deploy/local/run.sh`](../deploy/local/run.sh) |
| 로컬 Docker, 태스크 컨테이너 | 단일 호스트 | `LocalExecutor` | [`DockerOperator(@task.docker)`](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) | [`deploy/local/run-docker-task.sh`](../deploy/local/run-docker-task.sh) |
| kind + Helm | 로컬 단일 노드 Kubernetes | [`KubernetesExecutor`](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html) | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`deploy/kind/`](../deploy/kind/) |
| 외부 Kubernetes의 기존 Airflow | 외부 Kubernetes 클러스터 | 기존 인스턴스 설정 | [`PythonOperator(@task)`](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/operators.html) | [`src/scripts/experiment.py`](../src/scripts/experiment.py) |

표는 Operator 이름으로 통일했습니다. TaskFlow 문법에서 `@task`는 `PythonOperator`에, `@task.docker`는 `DockerOperator`에 대응합니다. [기준 DAG](../src/dags/portability.py)는 `@task`를 사용합니다.

태스크 컨테이너 시나리오는 같은 호스트에서 Docker 컨테이너 격리를 비교합니다.
기존 로컬 프로세스 실행과 DAG 단계, 입력, 기댓값을 맞추고, 실행 ID와 태스크 컨테이너의
이미지 및 ID를 기록합니다. kind는 [Helm values](../deploy/kubernetes/values.yaml)에 정의된 태스크별 Pod 방식을 사용합니다. 외부 환경에서는 이미 설치된 Airflow의 실제 Executor와 격리 단위를 기록합니다. [검증 계획](../PLAN-en.md)

### 순차 실행과 다국어 태스크

계획한 다국어 DAG에서는 각 태스크가 앞 단계의 완료를 기다렸다가 XCom을 읽고
다음 값을 기록합니다. 예를 들면 `start → start-python → start-python-typescript`입니다.
실행기가 태스크 위치를 결정한 뒤, 워커 안의 언어 코디네이터가 필요한 프로세스를 띄울 수
있습니다. TypeScript와 JavaScript는 Node.js, Java·Kotlin·Scala는 JVM, Go는 컴파일된
실행 파일을 사용합니다. 컨테이너나 워커 이미지에 필요한 런타임과 태스크 번들이 있어야
합니다. 문자열은 각 프로세스나 Pod 사이에서 XCom으로 주고받습니다.
[태스크 실행 아키텍처](https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html#task-execution-architecture) · [언어 코디네이터 설정](https://airflow.apache.org/docs/apache-airflow/3.4.0/configurations-ref.html#sdk) · [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

### Docker Engine에서 임시 컨테이너 실행

[태스크 컨테이너 실행 스크립트](../deploy/local/run-docker-task.sh)는 기준 DAG의 `sum_squares` 태스크에 Python 전용 작업 이미지를 사용합니다. 인증이 필요한 Airflow UI를 루프백 포트 8086에 열고, 워커를 Docker Engine에 연결합니다. 컨테이너 생성·실행·삭제 결과는 [이슈 #2](https://github.com/jyje/pilot-airflow-portability/issues/2)에 기록했습니다.

공식 Docker 프로바이더의 `DockerOperator`로 태스크마다 임시 컨테이너를 띄울 수
있습니다. `LocalExecutor` 구성에서는 Airflow 워커 프로세스가 오퍼레이터를 실행합니다.
오퍼레이터는 설정된 Docker Engine에 태스크 이미지로 컨테이너를 만들도록 요청하고,
명령이 끝날 때까지 기다려 결과를 수집합니다. `auto_remove="force"`를 설정하면
명령이 종료될 때 컨테이너를 삭제합니다. Python 함수에는 `@task.docker`도 사용할 수
있습니다. [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) · [Python Docker 태스크 데코레이터](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/decorators/docker.html)

Airflow 워커에는 Docker 소켓이나 API 엔드포인트를 통한 Engine 접근이 필요합니다.
호스트 Engine에 연결하면 작업용 컨테이너는 Airflow 컨테이너와 같은 Engine에서
별도로 실행됩니다. 현재 파일럿의 로컬 실행 스크립트는 Docker 소켓을 마운트하지
않습니다. [로컬 실행 스크립트](../deploy/local/run.sh) · [Docker 프로바이더 목록](https://airflow.apache.org/registry/providers/docker/)

## 그 밖의 실행기

| 실행기 | 실행 방식 |
| --- | --- |
| `CeleryExecutor` | 메시지 브로커에 작업을 보내고 상시 실행 중인 Celery 워커가 가져갑니다. 한 워커에서 여러 태스크 인스턴스를 처리할 수 있습니다. [Celery 실행기](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html) |
| `EdgeExecutor` | 원격 현장이나 엣지 장치의 워커가 HTTP(S)로 Airflow에서 작업을 가져옵니다. [Edge 실행기](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/edge_executor.html) |
| AWS 프로바이더 실행기 | `AwsBatchExecutor`는 AWS Batch 작업을 제출하고, `AwsEcsExecutor`는 ECS 태스크를 시작합니다. `AwsLambdaExecutor`는 Lambda를 호출하고, `AwsEksExecutor`는 태스크 Pod를 EKS에서 실행합니다. [커뮤니티 실행기 목록](https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/executors.html) · [Amazon 프로바이더 문서](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/executors/index.html) |
| 커스텀 실행기 | `BaseExecutor`를 구현하고 Python 클래스의 import 경로를 설정해 사용할 수 있습니다. [커스텀 실행기 작성](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html#writing-your-own-executor) |

Airflow 3에서는 `[core] executor`에 실행기를 여러 개 설정할 수 있습니다. 목록의
첫 실행기가 기본값이며, DAG나 태스크에서 설정된 다른 실행기를 선택할 수 있습니다.
이 구성이 기존 `CeleryKubernetesExecutor`와 `LocalKubernetesExecutor`를 대체합니다.
로컬 개발용 `SequentialExecutor`는 `LocalExecutor`로 대체됐습니다.
태스크를 순서대로 실행하려면 DAG에서 의존성을 연결합니다.
[복수 실행기 설정](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/executor/index.html#using-multiple-executors-concurrently) · [Airflow 3 업그레이드 가이드](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
