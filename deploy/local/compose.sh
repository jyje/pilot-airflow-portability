#!/usr/bin/env bash
set -euo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runtime="$here/.runtime"
mkdir -p "$runtime"/{dags,logs,config,plugins}
compose="$runtime/compose-3.1.8.yaml"
if [[ ! -s "$compose" ]]; then
    download=$(mktemp "$runtime/compose.XXXXXX")
    trap 'rm -f "$download"' EXIT
    curl --fail --silent --show-error --location \
        https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml \
        --output "$download"
    mv "$download" "$compose"
    trap - EXIT
fi

touch "$runtime/.env"
export ENV_FILE_PATH="$runtime/.env"
export AIRFLOW_IMAGE_NAME="${PILOT_IMAGE:-pilot-airflow-portability:3.1.8}"
export AIRFLOW_UID="${AIRFLOW_UID:-$(id -u)}"
exec docker compose --project-name pilot-airflow-portability --project-directory "$runtime" \
    --env-file "$ENV_FILE_PATH" -f "$compose" -f "$here/compose.override.yaml" "$@"
