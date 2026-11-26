#!/usr/bin/env bash
set -euo pipefail

context="${1:?Usage: install.sh KUBE_CONTEXT [HELM_ARGS...]}"
shift
namespace="${PILOT_NAMESPACE:-airflow-pilot}"
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/../.." && pwd)
dag_file="${PILOT_DAG_FILE:-$root/src/dags/portability.py}"
if [[ ! -f "$dag_file" || ! -r "$dag_file" ]]; then
    printf 'Cannot read the baseline DAG file: %s\n' "$dag_file" >&2
    exit 1
fi
chart=(airflow --repo https://airflow.apache.org --version 1.22.0)
if [[ -n "${PILOT_HELM_CHART:-}" ]]; then
    chart=("$PILOT_HELM_CHART")
fi

if [[ -z "$(kubectl --context "$context" get namespace "$namespace" --ignore-not-found -o name)" ]]; then
    kubectl --context "$context" create namespace "$namespace"
fi

dag_revision=$(
    kubectl --context "$context" --namespace "$namespace" create configmap airflow-pilot-dags \
        --from-file="portability.py=$dag_file" --dry-run=client -o yaml |
        kubectl --context "$context" --namespace "$namespace" apply -f - \
            -o jsonpath='{.metadata.resourceVersion}'
)

helm upgrade --install airflow-pilot "${chart[@]}" \
    --kube-context "$context" --namespace "$namespace" \
    -f "$here/values.yaml" --hide-notes --timeout 15m \
    --set-string "dagProcessor.podAnnotations.pilot-dag-revision=$dag_revision" "$@"

kubectl --context "$context" --namespace "$namespace" rollout status \
    deployment/airflow-pilot-api-server deployment/airflow-pilot-dag-processor \
    deployment/airflow-pilot-scheduler --timeout=8m
