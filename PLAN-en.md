# Plan

[English guide](docs/operations-en.md) / [한국어 가이드](docs/operations-ko.md)

Use one experiment with `items=10000` and `shards=1`. Run it once in each
environment, in the order below. The required result is `333283335000`.

- [x] Scaffold the Python 3.13 uv project, shared DAG, Typer script, comparison notebook, and deployment files.
- [x] Verify the initial scaffold with Ruff, ty, and five offline tests. Render its Compose and Helm configurations and execute its notebook with mock responses.
- [x] Document the single experiment and sequential rollout. Add centered English and Korean README navigation with the official Airflow logo.
- [ ] Prepare the shared image with Apache Airflow 3.4.0. Update the Typer command to submit one DAG run per invocation and the notebook to read one explicit run ID per environment. Re-run the offline checks for the updated flow.
- [ ] Start one local Airflow container with `airflow standalone`. Run the experiment once and record the run ID, terminal state, and result.
- [ ] Load the same study image into kind and deploy Airflow with Helm chart 1.22.0. Run the experiment once, verify the task Pod, and record the result.
- [ ] Deploy Airflow with the same Helm chart and study image on the agreed external Kubernetes context and namespace. Run the experiment once, verify the task Pod, and record the result.
- [ ] Execute the Jupyter notebook with all three run IDs. Reconcile the API results with the CLI output and record the portability verdict.
