#!/usr/bin/env bash
set -euo pipefail

name="${PILOT_LOCAL_CONTAINER:-pilot-airflow-docker-task}"
image="${PILOT_IMAGE:-pilot-airflow-portability:3.4.0-docker-tasks}"
task_image="${PILOT_DOCKER_TASK_IMAGE:-python:3.13-slim}"
port="${PILOT_LOCAL_PORT:-8086}"
socket="${PILOT_DOCKER_SOCKET:-/var/run/docker.sock}"
socket_group="${PILOT_DOCKER_SOCKET_GID:-0}"

bootstrap='
    set -eu
    umask 077
    if [ ! -f "$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE" ]; then
        python -c "import json, os, secrets; from pathlib import Path; Path(os.environ[\"AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE\"]).write_text(json.dumps(dict(admin=secrets.token_urlsafe(32))))"
    fi
    if [ -x /usr/local/bin/pilot-sdk-runtime ]; then
        export PYTHONPATH="$AIRFLOW__CORE__DAGS_FOLDER${PYTHONPATH:+:$PYTHONPATH}"
        AIRFLOW__SDK__COORDINATORS=$(pilot-sdk-runtime --configure)
        export AIRFLOW__SDK__COORDINATORS
    fi
    exec airflow standalone
'

exec docker run --detach --name "$name" \
    --publish "127.0.0.1:$port:8080" \
    --mount "type=bind,src=$socket,dst=/var/run/docker.sock,readonly" \
    --group-add "$socket_group" \
    --env AIRFLOW__CORE__LOAD_EXAMPLES=False \
    --env "AIRFLOW__CORE__PARALLELISM=${PILOT_LOCAL_PARALLELISM:-4}" \
    --env "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=${PILOT_DAG_IMPORT_TIMEOUT:-90}" \
    --env "AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=${PILOT_DAG_PROCESS_TIMEOUT:-120}" \
    --env "AIRFLOW__WORKERS__EXECUTION_API_TIMEOUT=${PILOT_EXECUTION_API_TIMEOUT:-45}" \
    --env AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=False \
    --env AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/pilot-passwords.json \
    --env "PILOT_DOCKER_TASK_IMAGE=$task_image" \
    "$image" bash -c "$bootstrap"
