#!/usr/bin/env bash
# ==============================================================================
# setup.sh — Zero-to-Ready Provisioning for a Brand-New GCP Project
# ==============================================================================
# Usage:
#   ./scripts/setup.sh <PROJECT_ID> [REGION] [BUCKET_NAME] [GCLOUD_ACCOUNT]
#
# Example:
#   ./scripts/setup.sh my-gcp-project-id us-central1
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
echo " Provisioning Vertex AI Batch + Sync Retry Demo Environment"
echo "------------------------------------------------------------------------"
echo " Project ID:   ${PROJECT_ID}"
echo " Region:       ${REGION}"
echo " GCS Bucket:   gs://${BUCKET_NAME}"
echo " Account:      ${ACCOUNT:-default}"
echo "========================================================================"

# 1. Enable required Google Cloud APIs
echo "[1/5] Enabling required APIs (aiplatform, storage, serviceusage)..."
gcloud services enable \
  aiplatform.googleapis.com \
  storage.googleapis.com \
  serviceusage.googleapis.com \
  --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}"

# 2. Provision the Vertex AI Service Agent (critical on brand-new GCP projects!)
echo "[2/5] Ensuring Vertex AI Service Agent identity exists..."
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)" "${ACCOUNT_FLAG[@]}")"
VERTEX_SA="service-${PROJECT_NUMBER}@gcp-sa-aiplatform.iam.gserviceaccount.com"

gcloud beta services identity create \
  --service=aiplatform.googleapis.com \
  --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}" >/dev/null 2>&1 || true

# 3. Create the Cloud Storage Bucket
echo "[3/5] Ensuring Cloud Storage bucket gs://${BUCKET_NAME} exists in ${REGION}..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}" >/dev/null 2>&1; then
  echo "      Bucket gs://${BUCKET_NAME} already exists."
else
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access \
    "${ACCOUNT_FLAG[@]}"
  echo "      Created bucket gs://${BUCKET_NAME}."
fi

# 4. Grant the Vertex AI Service Agent read/write access to the bucket
echo "[4/5] Granting Vertex AI Service Agent (${VERTEX_SA}) Storage Object Admin on gs://${BUCKET_NAME}..."
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="serviceAccount:${VERTEX_SA}" \
  --role="roles/storage.objectAdmin" \
  --project="${PROJECT_ID}" "${ACCOUNT_FLAG[@]}" >/dev/null

# 5. Create local Python virtual environment & install dependencies
echo "[5/5] Setting up Python virtual environment (.venv) and installing dependencies..."
python3 -m venv "${REPO_ROOT}/.venv"
"${REPO_ROOT}/.venv/bin/pip" install --upgrade --quiet pip
"${REPO_ROOT}/.venv/bin/pip" install --quiet -r "${REPO_ROOT}/requirements.txt"

echo ""
echo "========================================================================"
echo " Setup Complete! Run the demo with:"
echo "------------------------------------------------------------------------"
if [[ -n "${ACCOUNT}" ]]; then
  echo " ${REPO_ROOT}/.venv/bin/python ${REPO_ROOT}/src/batch_with_sync_retries.py \\"
  echo "   --project_id ${PROJECT_ID} \\"
  echo "   --region ${REGION} \\"
  echo "   --bucket ${BUCKET_NAME} \\"
  echo "   --account ${ACCOUNT}"
else
  echo " ${REPO_ROOT}/.venv/bin/python ${REPO_ROOT}/src/batch_with_sync_retries.py \\"
  echo "   --project_id ${PROJECT_ID} \\"
  echo "   --region ${REGION} \\"
  echo "   --bucket ${BUCKET_NAME}"
fi
echo "========================================================================"
