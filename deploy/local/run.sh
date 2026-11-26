#!/usr/bin/env bash
set -euo pipefail

name="${PILOT_LOCAL_CONTAINER:-pilot-airflow-local}"
image="${PILOT_IMAGE:-pilot-airflow-portability:3.4.0}"
port="${PILOT_LOCAL_PORT:-8080}"

exec docker run --detach --name "$name" \
    --publish "127.0.0.1:$port:8080" \
    --env AIRFLOW__CORE__LOAD_EXAMPLES=False \
    --env AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
    "$image" standalone
