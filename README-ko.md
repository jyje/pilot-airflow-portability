<div align="center">

# jyje/pilot-airflow-portability

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/airflow-site/main/landing-pages/site/static/images/airflow-logo-dark.svg">
  <img width="220" src="https://raw.githubusercontent.com/apache/airflow-site/main/landing-pages/site/static/images/airflow-logo.svg" alt="Apache Airflow">
</picture>

🚀 로컬 Docker와 외부 Kubernetes에서 Airflow DAG 이식성을 검증하는 파일럿 프로젝트

[English](README-en.md) / [한국어](README-ko.md)

</div>

## 시작하기

- [배포·검증 가이드](docs/operations-ko.md)에서 환경 구성, 실험 입력, 통과 기준을 확인합니다.
- [E03 태스크 Pod의 노드 배치](docs/e03-node-placement-ko.md).
- [진행 계획](PLAN-en.md)에서 검증 범위와 완료 기록을 확인합니다.
- [실험 스크립트](src/scripts/experiment.py)와 [비교 노트북](src/notebooks/01-compare-airflow.ipynb).

## 실행 시나리오

| 시나리오 | 작업 환경 | Executor | Operator (TaskFlow) |
| --- | --- | --- | --- |
| [E01](deploy/local/run.sh) | 로컬 Docker, 태스크 프로세스 | [`LocalExecutor`](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) | `PythonOperator(@task)` |
| [E02](deploy/local/run-docker-task.sh) | 단일 Docker 호스트, 태스크별 임시 컨테이너 | `LocalExecutor` | [`DockerOperator(@task.docker)`](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) |
| [E03](src/scripts/experiment.py) | 외부 Kubernetes의 기존 Airflow, 태스크별 Pod | [`KubernetesExecutor`](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html) | `PythonOperator(@task)` |

Operator는 기준 DAG의 예시입니다. [kind + Helm](deploy/kind/)은 검증 표 외의 선택 재현 환경입니다.

## 실험별 검증 현황

네 실험을 각 환경에서 한 번씩 수행합니다. 실행 ID, 각 태스크의 기대 상태, 결과값, 실행 격리 단위를 기록하면 완료입니다.

| 실험 | 기대 결과 | E01 | E02 | E03 |
| --- | --- | --- | --- | --- |
| 기준 DAG | 세 태스크 성공, 합계 `333283335000` | 완료 | 완료 | 완료 |
| 회귀형 정산 | `23 → 15 → 7 → 0`, 3회 반복 | 완료 | 완료 | 완료 |
| TypeScript 정산 | 순액 `220`, 게시·감사 성공, 격리 분기 스킵 | 완료 | 완료 | 재시도 후 완료 |
| 다중언어 순차 DAG | 태스크 9개와 XCom 값 9개 일치 | 완료 | 완료 | 재시도 후 완료 |

실험과 환경을 조합한 12개 검증이 모두 완료됐습니다.

[검증 결과와 실행 근거](docs/results-ko.md)

[실행기와 격리 방식 가이드](docs/executors-ko.md)
