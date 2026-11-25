# E03 task Pod placement

[English](e03-node-placement-en.md) / [한국어](e03-node-placement-ko.md)

The four E03 verification runs completed 32 successful task Pods. Node `raspi-50` ran 4 Pods, and `raspi-51` ran 28.

## Verification runs

| Experiment | Run ID | raspi-50 | raspi-51 | Success total |
| --- | --- | --- | --- | --- |
| Baseline | `pilot-e03-current-baseline-20261126` | 2 | 1 | 3 |
| Reconciliation loop | `pilot-e03-loop-20261125` | 0 | 14 | 14 |
| TypeScript settlement | `pilot-e03-typescript-20261125` | 0 | 6 | 6 |
| Mixed-language chain | `pilot-e03-mixed-20261125` | 2 | 7 | 9 |

## Task placement

| Experiment | Task ID | Node |
| --- | --- | --- |
| Baseline | `partition` | `raspi-50` |
| Baseline | `sum_squares` | `raspi-51` |
| Baseline | `total` | `raspi-50` |
| Reconciliation loop | `load_backlog` | `raspi-51` |
| Reconciliation loop | `summarize` | `raspi-51` |
| TypeScript settlement | `collect.sales` | `raspi-51` |
| TypeScript settlement | `collect.refunds` | `raspi-51` |
| TypeScript settlement | `reconcile` | `raspi-51` |
| TypeScript settlement | `can_publish` | `raspi-51` |
| TypeScript settlement | `publish` | `raspi-51` |
| TypeScript settlement | `audit` | `raspi-51` |
| Mixed-language chain | `start` | `raspi-51` |
| Mixed-language chain | `python` | `raspi-51` |
| Mixed-language chain | `typescript` | `raspi-51` |
| Mixed-language chain | `javascript` | `raspi-51` |
| Mixed-language chain | `go` | `raspi-51` |
| Mixed-language chain | `java` | `raspi-50` |
| Mixed-language chain | `kotlin` | `raspi-51` |
| Mixed-language chain | `scala` | `raspi-50` |
| Mixed-language chain | `yaml` | `raspi-51` |

## Reconciliation loop by iteration

The opening and summary tasks are listed above. Each iteration launched four task Pods.

| Task ID | Iteration 1 | Iteration 2 | Iteration 3 |
| --- | --- | --- | --- |
| `reconcile_batch.reserve_batch` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.apply_batch` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.measure` | `raspi-51` | `raspi-51` | `raspi-51` |
| `reconcile_batch.backlog_empty` | `raspi-51` | `raspi-51` | `raspi-51` |

## Failed attempts and skipped tasks

| Experiment | Task ID | Failed attempts | Node |
| --- | --- | --- | --- |
| TypeScript settlement | `collect.sales` | 1 | `raspi-51` |
| TypeScript settlement | `collect.refunds` | 1 | `raspi-51` |
| Mixed-language chain | `java` | 2 | `raspi-51` |

The TypeScript settlement task `quarantine` was skipped.

## How placement was verified

Airflow recorded the Pod hostname for each task instance. The hostname was matched to the `pod` label in historical Prometheus kubelet metrics. The `node` label identified the Kubernetes node. The metrics establish where each Pod ran. Comparing available capacity at scheduling time requires separate historical data.
