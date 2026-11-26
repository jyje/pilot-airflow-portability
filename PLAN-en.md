# Plan

[English guide](docs/operations-en.md) / [한국어 가이드](docs/operations-ko.md)

Verify the baseline, reconciliation loop, TypeScript settlement, and
mixed-language chain in E01 (local task processes), E02 (local Docker task
containers), and E03 (existing Airflow on external Kubernetes). kind is an
optional reproduction environment.

## Progress

- [x] Run the four experiments in all three required environments and record
  the results and execution isolation.
- [ ] Reconcile all 12 run IDs with the recorded CLI evidence.
- [x] Query the three current baseline run IDs in Jupyter and record the
  passing verdict and observed durations. [Jupyter results](docs/results-en.md#jupyter-baseline-comparison)
- [x] Run the baseline five times per environment with fixed inputs, verify each
  run in Jupyter, and record the observed distribution and infrastructure context. [Five-run results](docs/results-en.md#five-run-baseline-comparison)
- [x] Repeat reconciliation, TypeScript settlement, and the mixed-language chain
  five times per environment. Record each verification and publish one chart per workflow.
  [Repeated workflow results](docs/results-en.md#five-run-workflow-comparisons)
- [x] Apply the E03 Node.js startup timeout through GitOps and rerun only
  TypeScript settlement five times. [Cluster PR #426](https://github.com/jyje/cluster/pull/426) ·
  [E03 retest](docs/results-en.md#e03-retest-after-the-nodejs-timeout-change)
- [x] Audit task attempts, archive runs with errors, and replenish each
  timing series to five clean samples. Regenerate the comparison charts.
- [x] Collect 50 first-attempt reconciliation runs in each of E01, E02, and
  E03. Report the dedicated series with spread, mean uncertainty, and exclusions.

[Status matrix](README-en.md#verification-matrix) ·
[Run evidence (English)](docs/results-en.md) ·
[실행 근거 (한국어)](docs/results-ko.md)
