# Deployment and verification

[English](operations-en.md) / [한국어](operations-ko.md)

This guide describes the target flow. See [PLAN-en.md](../PLAN-en.md) for implementation
and live verification status. The existing deployment and CLI files still need
the single-experiment updates listed there.

## Shared experiment

Use one DAG, `pilot_portability`, and one input, `items=10000` and `shards=1`.
The DAG sums the squares of the integers from 0 to 9,999. Its final XCom must
equal `333283335000`. The Typer script submits one run to one Airflow endpoint,
waits for a terminal state, checks the final value, and records the run ID.
Run each environment once and confirm its result before moving on.

Build one shared image from `apache/airflow:3.4.0` with the DAG from
[`src/dags/`](../src/dags/). Record the built image identifier and use the same
build in all three environments. For hosts and nodes on different CPU
architectures, publish a multi-platform build and record the platform image
used in each environment.

Use a Python 3.13 uv environment from `src/` for the script and notebook.
Keep API URLs and bearer tokens in process variables or an ignored repository
`.env`. Give the script access to submit and read DAG runs and read XComs. Use
read access to DAG runs and XComs for the notebook.

## 1. Local Airflow container

Run the shared image as one Docker container with `airflow standalone`.
Publish the API port on loopback. Confirm that Airflow starts, the pilot DAG
loads, and the CLI can authenticate. Submit the local run and verify that the
DAG and all its tasks succeed and that the XCom matches the expected value.
Save the local run ID and CLI result.

## 2. kind with Airflow Helm

Load the shared image into the kind cluster. Install Apache Airflow Helm chart
`1.22.0` with `KubernetesExecutor`, explicitly setting the Airflow version to
`3.4.0` and selecting the shared image. Check the chart's Kubernetes and Helm
requirements before installing. Open a local port forward to its API service
and submit the kind run. Observe the task Pod during execution and save the
run ID and CLI result after verification.

## 3. External Kubernetes with Airflow Helm

Use the agreed kube context and namespace. Check API access, cluster
compatibility, and worker access to the shared image in the registry before
deployment. Use the same chart version, executor, and Airflow image as in
kind. Set environment-specific values for image credentials and storage when
required. Connect to the API through a port forward or an authorized HTTPS
endpoint. Submit the external run, confirm its task Pod and result, and save
the run ID and CLI output.

## Compare results in Jupyter

Open the [comparison notebook](../src/notebooks/01-compare-airflow.ipynb) and
supply the three recorded run IDs with their environments. Query the existing
runs and their XCom values through each Airflow API. Compare the notebook
records with the CLI output and include the following fields:

| Field | Verification |
| --- | --- |
| Environment and run ID | One known run for each environment |
| DAG and task status | Success in all three environments |
| Final XCom | `333283335000` in all three environments |
| Elapsed time | Report the Airflow start-to-end duration as an observation from a single run |

Record connection failures, timeouts, or missing results against the affected
environment and stop before advancing to the next environment. The experiment
passes when all three runs meet the success and result criteria.

## References

- [Airflow 3.4.0 release and container image](https://airflow.apache.org/blog/airflow-3.4.0/)
- [Airflow standalone quick start](https://airflow.apache.org/docs/apache-airflow/3.4.0/start.html)
- [Airflow Helm chart 1.22.0 and prerequisites](https://airflow.apache.org/docs/helm-chart/1.22.0/)
