# Goal

Define one portability experiment and run the same Airflow DAG with the same
input once per environment: a local Airflow container, kind, and external
Kubernetes. Run the environments in order with the same Typer script. Compare
the three recorded DAG runs in a read-only Jupyter notebook.
