#!/usr/bin/env bash
set -euo pipefail

# Tear down GCP infrastructure created by scripts/provision_infra.sh.
# Deletes selected Dataproc cluster(s) and benchmark BigQuery reservation/assignment
# by default; pass --keep-reservation to preserve reservations, or --purge-data
# to also delete BigQuery datasets, BigLake connection, GCS buckets (including
# dataproc-temp-* / dataproc-staging-*), and bench-* SA/VPC resources.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

CONFIRM="false"
PURGE_DATA="false"
CLUSTERS_MODE="both"
KEEP_RESERVATION="false"
FORCE_DELETE_RESERVATION="false"

usage() {
  cat <<'EOF'
Usage: scripts/teardown_infra.sh [OPTIONS]

Tears down benchmark Dataproc clusters and BigQuery slot reservation/assignment.

Options:
  --clusters=<both|std|lightning|none>   Which Dataproc cluster(s) to delete (default: both)
  --keep-reservation                     Skip deleting BigQuery reservation & QUERY assignment
  --force-delete-reservation             Allow deleting a reservation even if named sandbox-elt-cap
  --confirm                              Required in non-interactive runs; skips confirmation prompt
  --purge-data                           Also delete the 5 BigQuery datasets, BigLake connection,
                                         BENCH_BUCKET + dataproc-temp-*/dataproc-staging-* buckets,
                                         and bench-* SA / firewall / subnet / VPC resources
                                         (never touches airport-ops-* buckets or sandbox-* resources)
  -h, --help                             Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --clusters=*)
      CLUSTERS_MODE="${1#*=}"
      shift
      ;;
    --clusters)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --clusters requires an argument (both|std|lightning|none)" >&2
        exit 2
      fi
      CLUSTERS_MODE="$2"
      shift 2
      ;;
    --keep-reservation)
      KEEP_RESERVATION="true"
      shift
      ;;
    --force-delete-reservation)
      FORCE_DELETE_RESERVATION="true"
      shift
      ;;
    --confirm)
      CONFIRM="true"
      shift
      ;;
    --purge-data)
      PURGE_DATA="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "${CLUSTERS_MODE}" in
  both|std|lightning|none) ;;
  *)
    echo "ERROR: Invalid --clusters value '${CLUSTERS_MODE}'. Expected: both, std, lightning, or none." >&2
    exit 2
    ;;
esac

for bin in gcloud bq python3; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: Required CLI tool '${bin}' not found on PATH." >&2
    exit 1
  fi
done

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

BQ_LOCATION="${BQ_LOCATION:-US}"
BQ_LOCATION_LOWER="$(printf '%s' "${BQ_LOCATION}" | tr '[:upper:]' '[:lower:]')"
DATAPROC_REGION="${DATAPROC_REGION:-us-central1}"
CLUSTER_STD_NAME="${CLUSTER_STD_NAME:-bench-spark-std-x86}"
CLUSTER_LGT_NAME="${CLUSTER_LGT_NAME:-bench-spark-lightning-x86}"
BQ_RESERVATION_NAME="${BQ_RESERVATION_NAME:-bench-elt-slots}"
BIGLAKE_CONNECTION_ID="${BIGLAKE_CONNECTION_ID:-bench-biglake-${BQ_LOCATION_LOWER}}"
DS_BRONZE_NATIVE="${DS_BRONZE_NATIVE:-bench_bronze_native}"
DS_BRONZE_BIGLAKE="${DS_BRONZE_BIGLAKE:-bench_bronze_biglake}"
DS_SILVER_BQ="${DS_SILVER_BQ:-bench_silver_bq}"
DS_GOLD_BQ="${DS_GOLD_BQ:-bench_gold_bq}"
DS_METRICS="${DS_METRICS:-bench_metrics}"
BENCH_NETWORK="${BENCH_NETWORK:-bench-vpc}"
BENCH_SUBNET="${BENCH_SUBNET:-bench-pga-${DATAPROC_REGION}}"
DATAPROC_SA_NAME="${DATAPROC_SA_NAME:-bench-dataproc-runner}"

if [[ -z "${PROJECT_ID:-}" || "${PROJECT_ID}" == "your-gcp-project-id" ]]; then
  echo "ERROR: PROJECT_ID must be set to a real GCP project ID in .env or environment." >&2
  exit 1
fi

if [[ -z "${PROJECT_NUMBER:-}" ]]; then
  raw_proj_num="$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)" 2>&1)" || {
    echo "ERROR: Failed to resolve PROJECT_NUMBER for '${PROJECT_ID}': ${raw_proj_num}" >&2
    exit 1
  }
  PROJECT_NUMBER="$(printf '%s\n' "${raw_proj_num}" | grep -v '^Created ' | tr -d '[:space:]')"
fi

if [[ ! "${PROJECT_NUMBER}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: Invalid PROJECT_NUMBER '${PROJECT_NUMBER}' for PROJECT_ID='${PROJECT_ID}'." >&2
  exit 1
fi

BENCH_BUCKET="${BENCH_BUCKET:-lakehouse-bench-${PROJECT_NUMBER}}"
BQ_ADMIN_PROJECT_ID="${BQ_ADMIN_PROJECT_ID:-${PROJECT_ID}}"
DATAPROC_SA_EMAIL="${DATAPROC_SERVICE_ACCOUNT:-${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

# Validate every identifier as non-empty and free of wildcards/slashes before destructive calls.
validate_non_empty_id() {
  local label="$1"
  local val="$2"
  if [[ -z "${val}" || "${val}" == *"*"* || "${val}" == *"/"* || "${val}" =~ [[:space:]] ]]; then
    echo "ERROR: Unsafe or empty identifier for ${label}: '${val}'" >&2
    exit 1
  fi
}

validate_non_empty_id "PROJECT_ID" "${PROJECT_ID}"
validate_non_empty_id "DATAPROC_REGION" "${DATAPROC_REGION}"
validate_non_empty_id "CLUSTER_STD_NAME" "${CLUSTER_STD_NAME}"
validate_non_empty_id "CLUSTER_LGT_NAME" "${CLUSTER_LGT_NAME}"
validate_non_empty_id "BQ_ADMIN_PROJECT_ID" "${BQ_ADMIN_PROJECT_ID}"
validate_non_empty_id "BQ_LOCATION" "${BQ_LOCATION}"
validate_non_empty_id "BQ_RESERVATION_NAME" "${BQ_RESERVATION_NAME}"

CLUSTERS_TO_DELETE=()
case "${CLUSTERS_MODE}" in
  both)
    CLUSTERS_TO_DELETE=("${CLUSTER_STD_NAME}" "${CLUSTER_LGT_NAME}")
    ;;
  std)
    CLUSTERS_TO_DELETE=("${CLUSTER_STD_NAME}")
    ;;
  lightning)
    CLUSTERS_TO_DELETE=("${CLUSTER_LGT_NAME}")
    ;;
  none)
    CLUSTERS_TO_DELETE=()
    ;;
esac

ALL_DATASETS=(
  "${DS_BRONZE_BIGLAKE}"
  "${DS_BRONZE_NATIVE}"
  "${DS_SILVER_BQ}"
  "${DS_GOLD_BQ}"
  "${DS_METRICS}"
)

if [[ "${PURGE_DATA}" == "true" ]]; then
  validate_non_empty_id "BENCH_BUCKET" "${BENCH_BUCKET}"
  if [[ ! "${BENCH_BUCKET}" =~ ^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$ ]]; then
    echo "ERROR: Invalid GCS bucket name '${BENCH_BUCKET}'." >&2
    exit 1
  fi
  if [[ "${BENCH_BUCKET}" == airport-ops-* ]]; then
    echo "ERROR: Refusing to delete protected bucket '${BENCH_BUCKET}'." >&2
    exit 1
  fi
  validate_non_empty_id "BIGLAKE_CONNECTION_ID" "${BIGLAKE_CONNECTION_ID}"
  for ds in "${ALL_DATASETS[@]}"; do
    validate_non_empty_id "BigQuery dataset" "${ds}"
    if [[ ! "${ds}" =~ ^[A-Za-z0-9_]+$ ]]; then
      echo "ERROR: Invalid BigQuery dataset name '${ds}'." >&2
      exit 1
    fi
  done
fi

echo "==> Planned Teardown Targets"
echo "    Project:        ${PROJECT_ID} (${PROJECT_NUMBER})"
if [[ ${#CLUSTERS_TO_DELETE[@]} -gt 0 ]]; then
  echo "    Clusters:       ${CLUSTERS_TO_DELETE[*]} (${DATAPROC_REGION}, mode=${CLUSTERS_MODE})"
else
  echo "    Clusters:       NONE (--clusters=none)"
fi
if [[ "${KEEP_RESERVATION}" == "true" ]]; then
  echo "    BQ Reservation: PRESERVED (--keep-reservation)"
elif [[ "${BQ_RESERVATION_NAME}" == "sandbox-elt-cap" && "${FORCE_DELETE_RESERVATION}" != "true" ]]; then
  echo "    BQ Reservation: PROTECTED ('sandbox-elt-cap' skipped unless --force-delete-reservation)"
else
  echo "    BQ Reservation: ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME} + matching QUERY assignment"
fi
if [[ "${PURGE_DATA}" == "true" ]]; then
  echo "    PURGE DATA:     YES"
  echo "      - Datasets:   ${ALL_DATASETS[*]}"
  echo "      - Connection: ${PROJECT_ID}.${BQ_LOCATION}.${BIGLAKE_CONNECTION_ID}"
  echo "      - GCS Bucket: gs://${BENCH_BUCKET} (+ dataproc-temp-* / dataproc-staging-* buckets)"
  echo "      - Bench Infra: bench-* SA (${DATAPROC_SA_EMAIL}), bench-allow-internal, bench-pga-*, bench-vpc"
else
  echo "    PURGE DATA:     NO (datasets, BigLake connection, buckets, SA, and VPC preserved)"
fi

if [[ "${CONFIRM}" != "true" ]]; then
  if [[ ! -t 0 ]]; then
    echo "ERROR: Non-interactive shell detected without --confirm. Aborting teardown." >&2
    exit 1
  fi
  read -r -p "Type 'yes' to confirm deletion of the resources above: " reply
  if [[ "${reply}" != "yes" ]]; then
    echo "Aborted."
    exit 1
  fi
fi

# 1. Delete selected Dataproc clusters
for cluster in "${CLUSTERS_TO_DELETE[@]}"; do
  if gcloud dataproc clusters describe "${cluster}" --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "==> Deleting Dataproc cluster '${cluster}' (${DATAPROC_REGION})..."
    gcloud dataproc clusters delete "${cluster}" \
      --region="${DATAPROC_REGION}" \
      --project="${PROJECT_ID}" \
      --quiet
  else
    echo "    Dataproc cluster '${cluster}' not found; skipping."
  fi
done

# 2. Delete BigQuery reservation assignment & reservation (only matching BQ_RESERVATION_NAME)
if [[ "${KEEP_RESERVATION}" == "true" ]]; then
  echo "==> Skipping BigQuery reservation & assignment deletion (--keep-reservation set)."
elif [[ "${BQ_RESERVATION_NAME}" == "sandbox-elt-cap" && "${FORCE_DELETE_RESERVATION}" != "true" ]]; then
  echo "==> Skipping deletion of shared reservation 'sandbox-elt-cap' (pass --force-delete-reservation to override)."
else
  assignments_json="$(bq ls --reservation_assignment --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json 2>/dev/null || true)"
  ASSIGNMENT_ID="$(python3 - "${assignments_json}" "${PROJECT_ID}" "${PROJECT_NUMBER}" "${BQ_RESERVATION_NAME}" <<'PY'
import json
import sys

raw, project_id, project_number, target_res = sys.argv[1:5]
idx = raw.find("[")
if idx == -1:
    sys.exit(0)
try:
    items = json.loads(raw[idx:])
except json.JSONDecodeError:
    sys.exit(0)

targets = {f"projects/{project_id}", f"projects/{project_number}"}
for item in items:
    job_type = item.get("jobType") or item.get("job_type") or ""
    assignee = item.get("assignee") or ""
    if job_type != "QUERY" or assignee not in targets:
        continue
    name = item.get("name") or ""
    if "/assignments/" in name:
        parts = name.split("/")
        res_name = parts[-3] if len(parts) >= 3 else ""
        if res_name == target_res:
            print(f"{res_name}.{parts[-1]}")
            sys.exit(0)
    elif "." in name:
        tail = name.split(":", 1)[-1]
        parts = tail.split(".")
        if len(parts) >= 2 and parts[-2] == target_res:
            print(name)
            sys.exit(0)
PY
)"

  if [[ -n "${ASSIGNMENT_ID}" ]]; then
    echo "==> Deleting BigQuery reservation assignment '${ASSIGNMENT_ID}'..."
    bq rm --reservation_assignment \
      --project_id="${BQ_ADMIN_PROJECT_ID}" \
      --location="${BQ_LOCATION}" \
      "${ASSIGNMENT_ID}"
  else
    echo "    No matching QUERY reservation assignment found for ${BQ_RESERVATION_NAME}; skipping."
  fi

  # 3. Scale down baseline/autoscale slots to 0 (if non-zero) and delete BigQuery Enterprise reservation
  if res_json="$(bq show --reservation --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BQ_RESERVATION_NAME}" 2>/dev/null)"; then
    need_zero="$(python3 - "${res_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
if idx == -1:
    print("0")
    sys.exit(0)
obj = json.loads(raw[idx:])
baseline = int(obj.get("slotCapacity") or 0)
autoscale_max = int(((obj.get("autoscale") or {}).get("maxSlots")) or 0)
print("1" if (baseline > 0 or autoscale_max > 0) else "0")
PY
)"
    if [[ "${need_zero}" == "1" ]]; then
      echo "==> Scaling BigQuery reservation '${BQ_RESERVATION_NAME}' to 0 baseline / 0 autoscale slots before deletion..."
      bq update --reservation \
        --project_id="${BQ_ADMIN_PROJECT_ID}" \
        --location="${BQ_LOCATION}" \
        --slots=0 \
        --autoscale_max_slots=0 \
        "${BQ_RESERVATION_NAME}"
    fi
    echo "==> Deleting BigQuery reservation '${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME}'..."
    bq rm --reservation \
      --project_id="${BQ_ADMIN_PROJECT_ID}" \
      --location="${BQ_LOCATION}" \
      "${BQ_RESERVATION_NAME}"
  else
    echo "    BigQuery reservation '${BQ_RESERVATION_NAME}' not found; skipping."
  fi
fi

# 4. Optional data purge (--purge-data)
delete_bucket_completely() {
  local bucket_name="$1"
  if [[ -z "${bucket_name}" || "${bucket_name}" == airport-ops-* ]]; then
    return 0
  fi
  if [[ ! "${bucket_name}" =~ ^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$ ]]; then
    echo "    [WARN] Skipping invalid bucket name '${bucket_name}'." >&2
    return 0
  fi
  if gcloud storage buckets describe "gs://${bucket_name}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "    Clearing soft-delete retention on gs://${bucket_name}..."
    gcloud storage buckets update "gs://${bucket_name}" \
      --project="${PROJECT_ID}" \
      --soft-delete-duration=0 >/dev/null 2>&1 || true
    echo "==> Deleting GCS bucket and all object versions: gs://${bucket_name}..."
    gcloud storage rm --recursive --quiet "gs://${bucket_name}"
  fi
}

if [[ "${PURGE_DATA}" == "true" ]]; then
  for ds in "${ALL_DATASETS[@]}"; do
    if bq show --project_id="${PROJECT_ID}" --format=none "${PROJECT_ID}:${ds}" >/dev/null 2>&1; then
      echo "==> Deleting BigQuery dataset '${PROJECT_ID}:${ds}'..."
      bq rm -r -f -d "${PROJECT_ID}:${ds}"
    else
      echo "    Dataset '${PROJECT_ID}:${ds}' not found; skipping."
    fi
  done

  if bq show --connection --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BIGLAKE_CONNECTION_ID}" >/dev/null 2>&1; then
    echo "==> Deleting BigLake connection '${BIGLAKE_CONNECTION_ID}' (${BQ_LOCATION})..."
    bq rm --connection \
      --project_id="${PROJECT_ID}" \
      --location="${BQ_LOCATION}" \
      "${BIGLAKE_CONNECTION_ID}"
  else
    echo "    BigLake connection '${BIGLAKE_CONNECTION_ID}' not found; skipping."
  fi

  if gcloud storage buckets describe "gs://${BENCH_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    delete_bucket_completely "${BENCH_BUCKET}"
  else
    echo "    GCS bucket 'gs://${BENCH_BUCKET}' not found; skipping."
  fi

  # Discover and delete Dataproc-generated temp/staging buckets (never airport-ops-*)
  raw_buckets="$(gcloud storage buckets list --project="${PROJECT_ID}" --format="value(name)" 2>/dev/null || true)"
  if [[ -n "${raw_buckets}" ]]; then
    while IFS= read -r dp_bucket; do
      dp_bucket="$(printf '%s' "${dp_bucket}" | tr -d '[:space:]')"
      [[ -z "${dp_bucket}" || "${dp_bucket}" == "Created" ]] && continue
      [[ "${dp_bucket}" == "${BENCH_BUCKET}" ]] && continue
      [[ "${dp_bucket}" == airport-ops-* ]] && continue
      if [[ "${dp_bucket}" == dataproc-temp-* || "${dp_bucket}" == dataproc-staging-* ]]; then
        delete_bucket_completely "${dp_bucket}"
      fi
    done <<<"$(printf '%s\n' "${raw_buckets}" | grep -v '^Created ' || true)"
  fi

  # Clean up benchmark-owned dedicated SA (bench-* only)
  sa_short="${DATAPROC_SA_EMAIL%%@*}"
  if [[ "${sa_short}" == bench-* && "${DATAPROC_SA_EMAIL}" == *"@${PROJECT_ID}.iam.gserviceaccount.com" ]]; then
    if gcloud iam service-accounts describe "${DATAPROC_SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
      echo "==> Removing project IAM binding and deleting benchmark SA ${DATAPROC_SA_EMAIL}..."
      gcloud projects remove-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
        --role="roles/dataproc.worker" \
        --condition=None >/dev/null 2>&1 || true
      gcloud iam service-accounts delete "${DATAPROC_SA_EMAIL}" \
        --project="${PROJECT_ID}" \
        --quiet
    else
      echo "    Benchmark SA '${DATAPROC_SA_EMAIL}' not found; skipping."
    fi
  else
    echo "    Preserving non-benchmark SA '${DATAPROC_SA_EMAIL}'."
  fi

  # Clean up benchmark-owned VPC resources (bench-* only; never touch sandbox-* or default)
  fw_json="$(gcloud compute firewall-rules list --project="${PROJECT_ID}" --format=json 2>/dev/null || true)"
  mapfile -t BENCH_FW_RULES < <(python3 - "${fw_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("[")
rules = json.loads(raw[idx:]) if idx != -1 else []
for rule in rules:
    name = rule.get("name") or ""
    net = (rule.get("network") or "").rstrip("/").split("/")[-1]
    if name.startswith("bench-") and net.startswith("bench-"):
        print(name)
PY
)
  for fw_name in "${BENCH_FW_RULES[@]}"; do
    [[ -z "${fw_name}" ]] && continue
    echo "==> Deleting benchmark firewall rule '${fw_name}'..."
    gcloud compute firewall-rules delete "${fw_name}" --project="${PROJECT_ID}" --quiet
  done

  subnet_list_json="$(gcloud compute networks subnets list --project="${PROJECT_ID}" --regions="${DATAPROC_REGION}" --format=json 2>/dev/null || true)"
  mapfile -t BENCH_SUBNETS < <(python3 - "${subnet_list_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("[")
items = json.loads(raw[idx:]) if idx != -1 else []
for item in items:
    name = item.get("name") or ""
    net = (item.get("network") or "").rstrip("/").split("/")[-1]
    if name.startswith("bench-") and net.startswith("bench-"):
        print(name)
PY
)
  for sub_name in "${BENCH_SUBNETS[@]}"; do
    [[ -z "${sub_name}" ]] && continue
    echo "==> Deleting benchmark subnet '${sub_name}' (${DATAPROC_REGION})..."
    gcloud compute networks subnets delete "${sub_name}" \
      --region="${DATAPROC_REGION}" \
      --project="${PROJECT_ID}" \
      --quiet
  done

  if [[ "${BENCH_NETWORK}" == bench-* ]]; then
    if net_json="$(gcloud compute networks describe "${BENCH_NETWORK}" --project="${PROJECT_ID}" --format=json 2>/dev/null)"; then
      remaining_subnets="$(python3 - "${net_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
if idx == -1:
    print("0")
    sys.exit(0)
obj = json.loads(raw[idx:])
print(len(obj.get("subnetworks") or []))
PY
)"
      if [[ "${remaining_subnets}" == "0" ]]; then
        echo "==> Deleting benchmark VPC '${BENCH_NETWORK}'..."
        gcloud compute networks delete "${BENCH_NETWORK}" --project="${PROJECT_ID}" --quiet
      else
        echo "    Preserving VPC '${BENCH_NETWORK}' (${remaining_subnets} subnet(s) in other regions remain)."
      fi
    fi
  fi
fi

echo "==> Teardown complete."
