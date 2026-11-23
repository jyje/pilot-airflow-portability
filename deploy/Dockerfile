FROM apache/airflow:3.1.8
COPY --chown=airflow:root src/dags/portability.py /opt/airflow/pilot-dags/portability.py
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/pilot-dags
