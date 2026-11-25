# E03 태스크 Pod의 노드 배치

[English](e03-node-placement-en.md) / [한국어](e03-node-placement-ko.md)

E03 검증 실행 네 건에서 태스크 Pod 32개가 성공했습니다. `raspi-50`에서 4개, `raspi-51`에서 28개가 실행됐습니다.

## 검증 실행

| 실험 | 실행 ID | raspi-50 | raspi-51 | 성공 합계 |
| --- | --- | --- | --- | --- |
| 기준 DAG | `pilot-e03-current-baseline-20261126` | 2 | 1 | 3 |
| 회귀형 정산 | `pilot-e03-loop-20261125` | 0 | 14 | 14 |
| TypeScript 정산 | `pilot-e03-typescript-20261125` | 0 | 6 | 6 |
| 다중언어 체인 | `pilot-e03-mixed-20261125` | 2 | 7 | 9 |

## 태스크별 실행 노드

| 실험 | 태스크 ID | 실행 노드 |
| --- | --- | --- |
| 기준 DAG | `partition` | `raspi-50` |
| 기준 DAG | `sum_squares` | `raspi-51` |
| 기준 DAG | `total` | `raspi-50` |
| 회귀형 정산 | `load_backlog` | `raspi-51` |
| 회귀형 정산 | `summarize` | `raspi-51` |
| TypeScript 정산 | `collect.sales` | `raspi-51` |
| TypeScript 정산 | `collect.refunds` | `raspi-51` |
| TypeScript 정산 | `reconcile` | `raspi-51` |
| TypeScript 정산 | `can_publish` | `raspi-51` |
| TypeScript 정산 | `publish` | `raspi-51` |
| TypeScript 정산 | `audit` | `raspi-51` |
| 다중언어 체인 | `start` | `raspi-51` |
| 다중언어 체인 | `python` | `raspi-51` |
| 다중언어 체인 | `typescript` | `raspi-51` |
| 다중언어 체인 | `javascript` | `raspi-51` |
| 다중언어 체인 | `go` | `raspi-51` |
| 다중언어 체인 | `java` | `raspi-50` |
| 다중언어 체인 | `kotlin` | `raspi-51` |
| 다중언어 체인 | `scala` | `raspi-50` |
| 다중언어 체인 | `yaml` | `raspi-51` |

## 회귀형 정산 회차별 실행 노드

시작과 집계 태스크는 위 표에 있습니다. 각 회차에는 태스크 Pod가 네 개씩 실행됐습니다.

| 태스크 ID | 1회차 | 2회차 | 3회차 |
| --- | --- | --- | --- |
| `reconcile_batch.reserve_batch` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.apply_batch` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.measure` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.backlog_empty` | `raspi-51` | `raspi-51` | `raspi-51` |

## 실패한 시도와 스킵된 태스크

| 실험 | 태스크 ID | 실패 횟수 | 실행 노드 |
| --- | --- | --- | --- |
| TypeScript 정산 | `collect.sales` | 1 | `raspi-51` |
| TypeScript 정산 | `collect.refunds` | 1 | `raspi-51` |
| 다중언어 체인 | `java` | 2 | `raspi-51` |

TypeScript 정산의 `quarantine` 태스크는 스킵됐습니다.

## 배치 확인 방법

Airflow가 기록한 태스크 인스턴스별 Pod 호스트 이름을 Prometheus의 과거 kubelet 메트릭에 있는 `pod` 라벨과 대조했습니다. 실제 Kubernetes 노드는 `node` 라벨에서 확인했습니다. 배치 당시의 리소스 여유를 비교하려면 당시 지표가 별도로 필요합니다.
