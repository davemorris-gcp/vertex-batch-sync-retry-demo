#!/usr/bin/env bash
# ==============================================================================
# teardown.sh — Clean Up All GCP & Local Resources Created by the Demo
# ==============================================================================
# Usage:
#   ./scripts/teardown.sh <PROJECT_ID> [REGION] [BUCKET_NAME] [GCLOUD_ACCOUNT]
# ==============================================================================
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <PROJECT_ID> [REGION] [BUCKET_NAME] [GCLOUD_ACCOUNT]"
  exit 1
fi

PROJECT_ID="$1"
REGION="${2:-us-central1}"
BUCKET_NAME="${3:-${PROJECT_ID}-vertex-batch-demo}"
ACCOUNT="${4:-$(gcloud config get-value account 2>/dev/null || true)}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACCOUNT_FLAG=()
if [[ -n "${ACCOUNT}" ]]; then
  ACCOUNT_FLAG=("--account=${ACCOUNT}")
fi

echo "========================================================================"
echo " Tearing Down Vertex AI Batch + Sync Retry Demo Resources"
echo "------------------------------------------------------------------------"
echo " Project ID:   ${PROJECT_ID}"
echo " Region:       ${REGION}"
echo " GCS Bucket:   gs://${BUCKET_NAME}"
echo " Account:      ${ACCOUNT:-default}"
echo "========================================================================"

# 1. Cancel and delete demo BatchPredictionJobs in Vertex AI
echo "[1/3] Cleaning up demo BatchPredictionJobs in ${REGION}..."
ACCESS_TOKEN="$(gcloud auth print-access-token "${ACCOUNT_FLAG[@]}")"
API_BASE="https://${REGION}-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/batchPredictionJobs"

JOBS_JSON="$(curl -s -H "Authorization: Bearer ${ACCESS_TOKEN}" "${API_BASE}" || echo "{}")"
JOB_NAMES="$(python3 -c '
import json, sys
data = json.loads(sys.stdin.read() or "{}")
for job in data.get("batchPredictionJobs", []):
    if str(job.get("displayName", "")).startswith("genai-sdk-batch-sync-retry-"):
        print(f"{job[\"name\"]}|{job.get(\"state\", \"\")}")
' <<< "${JOBS_JSON}")"

if [[ -z "${JOB_NAMES}" ]]; then
  echo "      No matching demo BatchPredictionJobs found."
else
  while IFS="|" read -r JOB_NAME JOB_STATE; do
    [[ -z "${JOB_NAME}" ]] && continue
    JOB_ID="${JOB_NAME##*/}"
    if [[ "${JOB_STATE}" == "JOB_STATE_PENDING" || "${JOB_STATE}" == "JOB_STATE_QUEUED" || "${JOB_STATE}" == "JOB_STATE_RUNNING" ]]; then
      echo "      Cancelling active BatchPredictionJob ${JOB_ID} (${JOB_STATE})..."
      curl -s -X POST -H "Authorization: Bearer ${ACCESS_TOKEN}" \
        "https://${REGION}-aiplatform.googleapis.com/v1/${JOB_NAME}:cancel" >/dev/null || true
    fi
    echo "      Deleting BatchPredictionJob ${JOB_ID}..."
    curl -s -X DELETE -H "Authorization: Bearer ${ACCESS_TOKEN}" \
      "https://${REGION}-aiplatform.googleapis.com/v1/${JOB_NAME}" >/dev/null || true
  done <<< "${JOB_NAMES}"
fi

# 2. Delete the Cloud Storage bucket and all objects inside it
echo "[2/3] Removing Cloud Storage bucket gs://${BUCKET_NAME} and all contents..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}" >/dev/null 2>&1; then
  gcloud storage rm -r "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}"
  echo "      Deleted bucket gs://${BUCKET_NAME}."
else
  echo "      Bucket gs://${BUCKET_NAME} already removed or not found."
fi

# 3. Remove local Python virtual environment if present
echo "[3/3] Cleaning up local virtual environment (.venv)..."
if [[ -d "${REPO_ROOT}/.venv" ]]; then
  rm -rf "${REPO_ROOT}/.venv"
  echo "      Removed ${REPO_ROOT}/.venv."
else
  echo "      No local .venv found."
fi

echo "========================================================================"
echo " Teardown Complete! All demo resources have been removed."
echo "========================================================================"
