# 배포 및 검증

[English](operations-en.md) / [한국어](operations-ko.md)

이 가이드는 목표 실행 절차를 설명합니다. 구현과 실제 검증 상태는
[PLAN-en.md](../PLAN-en.md)에서 확인할 수 있습니다. 현재 배포 파일과 CLI에는
계획에 기록한 단일 실험 절차를 적용해야 합니다.

## 공통 실험

DAG는 `pilot_portability` 하나를 사용하고 입력은 `items=10000`, `shards=1`로 고정합니다.
DAG는 0부터 9,999까지 각 정수의 제곱을 합산합니다. 최종 XCom의 기댓값은
`333283335000`입니다. Typer 스크립트는 Airflow 엔드포인트 한 곳에 DAG 실행을 한 번
요청하고, 종료 상태와 결과값을 확인한 뒤 실행 ID를 기록합니다. 환경별로 한 번 실행하고,
결과를 확인한 뒤 다음 환경으로 넘어갑니다.

`apache/airflow:3.4.0`을 기반으로 [`src/dags/`](../src/dags/)의 DAG를 포함한 공통 이미지를
빌드합니다. 이미지 식별자를 기록하고 세 환경에서 같은 빌드를 사용합니다. 호스트와 노드의
CPU 아키텍처가 다르면 멀티 플랫폼 이미지를 발행하고 환경별로 사용한 플랫폼 이미지를 기록합니다.

스크립트와 노트북은 `src/`에서 Python 3.13 uv 환경으로 실행합니다.
API URL과 Bearer 토큰은 프로세스 환경 변수나 Git에서 제외된 저장소 루트의 `.env`에 둡니다.
스크립트에는 DAG 실행 요청, 실행 이력 조회, XCom 읽기 권한을 부여합니다. 노트북에는
실행 이력과 XCom 읽기 권한을 부여합니다.

## 1. 로컬 Airflow 컨테이너

공통 이미지를 Docker 컨테이너 하나에서 `airflow standalone`으로 실행합니다.
API 포트는 루프백에 연결합니다. Airflow 시작, 파일럿 DAG 로딩, CLI 인증을 확인합니다.
로컬 실험을 실행해 DAG와 모든 태스크가 성공하고 XCom이 기댓값과 일치하는지 확인합니다.
로컬 실행 ID와 CLI 결과를 저장합니다.

## 2. kind와 Airflow Helm

kind 클러스터에 공통 이미지를 로드합니다. Apache Airflow Helm 차트 `1.22.0`을
`KubernetesExecutor`로 설치하고 Airflow 버전 `3.4.0`과 공통 이미지를 명시합니다.
설치 전에 차트가 요구하는 Kubernetes와 Helm 버전을 확인합니다. API 서비스에 로컬
포트 포워딩을 열고 kind 실험을 실행합니다. 실행 중 태스크 Pod를 관찰하고 결과를
검증한 뒤 실행 ID와 CLI 결과를 저장합니다.

## 3. 외부 Kubernetes와 Airflow Helm

합의한 kube context와 namespace를 사용합니다. 배포 전에 API 접근, 클러스터 호환성,
워커 노드의 이미지 레지스트리 접근을 확인합니다. kind와 같은 차트 버전, executor,
Airflow 이미지를 사용합니다. 필요하면 이미지 인증과 스토리지에 환경별 값을 적용합니다.
포트 포워딩 또는 승인된 HTTPS 엔드포인트로 API에 연결합니다. 외부 환경 실험을 실행하고
태스크 Pod와 결과를 확인한 뒤 실행 ID와 CLI 결과를 저장합니다.

## Jupyter로 결과 비교

[비교 노트북](../src/notebooks/01-compare-airflow.ipynb)을 열고 환경별로 기록한 실행 ID
세 개를 입력합니다. 각 Airflow API에서 기존 실행과 XCom 값을 조회합니다. 노트북 조회
결과와 CLI 기록을 대조하고 다음 항목을 남깁니다.

| 항목 | 확인 내용 |
| --- | --- |
| 환경과 실행 ID | 환경별로 기록한 실행 한 건 |
| DAG와 태스크 상태 | 세 환경 모두 성공 |
| 최종 XCom | 세 환경 모두 `333283335000` |
| 소요 시간 | Airflow 시작부터 종료까지의 시간을 단일 실행의 관측값으로 기록 |

연결 실패, 시간 초과, 결과 누락이 있으면 해당 환경에 기록하고 다음 환경으로 넘어가기 전에
멈춥니다. 세 환경에서 성공 상태와 결과값을 모두 확인하면 실험을 통과합니다.

## 참고 자료

- [Airflow 3.4.0 릴리스와 컨테이너 이미지](https://airflow.apache.org/blog/airflow-3.4.0/)
- [Airflow standalone 빠른 시작](https://airflow.apache.org/docs/apache-airflow/3.4.0/start.html)
- [Airflow Helm 차트 1.22.0과 요구 사항](https://airflow.apache.org/docs/helm-chart/1.22.0/)
