#!/usr/bin/env bash
set -euo pipefail

# Provision GCP infrastructure for the Lakehouse Compute Benchmark:
#   - Required GCP APIs
#   - GCS bucket (UBLA, PAP, zero soft-delete retention) + BigLake connection + IAM
#   - 5 BigQuery datasets (PHYSICAL storage billing on native/silver/gold) + metrics table
#   - BigQuery Enterprise Edition autoscale slot reservation + QUERY assignment (or safe reuse)
#   - Isolated custom VPC / Private Google Access subnet (never mutates default unless requested),
#     internal firewall, dedicated least-privilege Dataproc SA, quota preflight, & auto-terminating clusters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

CLUSTERS_MODE="both"
SKIP_RESERVATION="false"
REUSE_RESERVATION="false"
REUSE_RESERVATION_NAME=""
BENCH_NETWORK_ARG=""
BENCH_SUBNET_ARG=""
CREATE_SUBNET="false"

usage() {
  cat <<'EOF'
Usage: scripts/provision_infra.sh [OPTIONS]

Idempotently provisions GCP resources configured in .env (or environment variables).

Options:
  --clusters=<both|std|lightning|none>   Which Dataproc GCE x86 clusters to create (default: both)
  --skip-reservation                     Skip BigQuery Enterprise slot reservation & assignment
  --reuse-reservation[=<name>]           Verify existing BigQuery reservation & QUERY assignment
                                         without resizing, patching, or overwriting it (safe for
                                         shared reservations such as sandbox-elt-cap)
  --network=<name>                       VPC network name for cluster subnet creation (default: bench-vpc)
  --subnet=<name>                        Existing subnet (or target subnet with --create-subnet).
                                         Never modifies 'default' unless --subnet=default is passed.
  --create-subnet                        Create custom VPC & Private Google Access subnet if missing
                                         (no Cloud NAT; defaults to bench-vpc / bench-pga-<region>)
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
    --skip-reservation)
      SKIP_RESERVATION="true"
      shift
      ;;
    --reuse-reservation=*)
      REUSE_RESERVATION="true"
      REUSE_RESERVATION_NAME="${1#*=}"
      if [[ -z "${REUSE_RESERVATION_NAME}" ]]; then
        echo "ERROR: --reuse-reservation= requires a non-empty reservation name." >&2
        exit 2
      fi
      shift
      ;;
    --reuse-reservation)
      REUSE_RESERVATION="true"
      if [[ $# -ge 2 && "$2" != -* ]]; then
        REUSE_RESERVATION_NAME="$2"
        shift 2
      else
        shift
      fi
      ;;
    --network=*)
      BENCH_NETWORK_ARG="${1#*=}"
      shift
      ;;
    --network)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --network requires a VPC network name" >&2
        exit 2
      fi
      BENCH_NETWORK_ARG="$2"
      shift 2
      ;;
    --subnet=*)
      BENCH_SUBNET_ARG="${1#*=}"
      shift
      ;;
    --subnet)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --subnet requires a subnet name" >&2
        exit 2
      fi
      BENCH_SUBNET_ARG="$2"
      shift 2
      ;;
    --create-subnet)
      CREATE_SUBNET="true"
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

if [[ "${SKIP_RESERVATION}" == "true" && "${REUSE_RESERVATION}" == "true" ]]; then
  echo "ERROR: --skip-reservation and --reuse-reservation are mutually exclusive." >&2
  exit 2
fi

for bin in gcloud bq python3; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: Required CLI tool '${bin}' not found on PATH." >&2
    exit 1
  fi
done

if [[ -f "${ENV_FILE}" && "${ENV_FILE}" != "/dev/null" ]]; then
  eval "$(python3 "${REPO_ROOT}/scripts/metrics_helper.py" --export-unset "${ENV_FILE}")"
fi

BQ_LOCATION="${BQ_LOCATION:-US}"
GCS_LOCATION="${GCS_LOCATION:-${BQ_LOCATION}}"
BQ_LOCATION_LOWER="$(printf '%s' "${BQ_LOCATION}" | tr '[:upper:]' '[:lower:]')"
GCS_LOCATION_LOWER="$(printf '%s' "${GCS_LOCATION}" | tr '[:upper:]' '[:lower:]')"
if [[ "${BQ_LOCATION_LOWER}" != "us" && "${GCS_LOCATION_LOWER}" != "${BQ_LOCATION_LOWER}" ]]; then
  echo "ERROR: Non-US BQ_LOCATION='${BQ_LOCATION}' requires co-located GCS_LOCATION='${BQ_LOCATION}' (got GCS_LOCATION='${GCS_LOCATION}')." >&2
  echo "       Refusing to create or use a mismatched/US bucket under constraints/gcp.resourceLocations." >&2
  exit 1
fi
DATAPROC_REGION="${DATAPROC_REGION:-us-central1}"
DATAPROC_ZONE="${DATAPROC_ZONE:-us-central1-a}"
DATAPROC_IMAGE_VERSION="${DATAPROC_IMAGE_VERSION:-2.3-debian12}"
CLUSTER_STD_NAME="${CLUSTER_STD_NAME:-bench-spark-std-x86}"
CLUSTER_LGT_NAME="${CLUSTER_LGT_NAME:-bench-spark-lightning-x86}"
MASTER_MACHINE_TYPE="${MASTER_MACHINE_TYPE:-n2d-standard-4}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n2d-standard-8}"
NUM_PRIMARY_WORKERS="${NUM_PRIMARY_WORKERS:-2}"
NUM_WORKER_LOCAL_SSDS="${NUM_WORKER_LOCAL_SSDS:-1}"
CLUSTER_MAX_IDLE="${CLUSTER_MAX_IDLE:-30m}"
CLUSTER_MAX_AGE="${CLUSTER_MAX_AGE:-4h}"
BQ_RESERVATION_NAME="${BQ_RESERVATION_NAME:-bench-elt-slots}"
BQ_BASELINE_SLOTS="${BQ_BASELINE_SLOTS:-0}"
BQ_SLOT_CAP="${BQ_SLOT_CAP:-100}"
BIGLAKE_CONNECTION_ID="${BIGLAKE_CONNECTION_ID:-bench-biglake-${BQ_LOCATION_LOWER}}"
DS_BRONZE_NATIVE="${DS_BRONZE_NATIVE:-bench_bronze_native}"
DS_BRONZE_BIGLAKE="${DS_BRONZE_BIGLAKE:-bench_bronze_biglake}"
DS_SILVER_BQ="${DS_SILVER_BQ:-bench_silver_bq}"
DS_GOLD_BQ="${DS_GOLD_BQ:-bench_gold_bq}"
DS_METRICS="${DS_METRICS:-bench_metrics}"

BENCH_NETWORK="${BENCH_NETWORK_ARG:-${BENCH_NETWORK:-bench-vpc}}"
BENCH_SUBNET="${BENCH_SUBNET_ARG:-${BENCH_SUBNET:-}}"
BENCH_SUBNET_CIDR="${BENCH_SUBNET_CIDR:-10.20.0.0/24}"
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

if [[ "${REUSE_RESERVATION}" == "true" ]]; then
  REUSE_RESERVATION_NAME="${REUSE_RESERVATION_NAME:-${BQ_RESERVATION_NAME}}"
  BQ_RESERVATION_NAME="${REUSE_RESERVATION_NAME}"
fi

DATAPROC_SA_EMAIL="${DATAPROC_SERVICE_ACCOUNT:-${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"
if [[ "${DATAPROC_SA_EMAIL}" =~ ^[0-9]+-compute@developer\. ]] || [[ "${DATAPROC_SA_EMAIL}" == *-compute@developer.* ]]; then
  echo "ERROR: Default Compute Engine service account '${DATAPROC_SA_EMAIL}' is disallowed." >&2
  echo "       Configure dedicated least-privilege SA via DATAPROC_SA_NAME (default: bench-dataproc-runner)" >&2
  echo "       or pass a non-default DATAPROC_SERVICE_ACCOUNT." >&2
  exit 1
fi

echo "==> Provisioning benchmark infrastructure"
echo "    PROJECT_ID=${PROJECT_ID} (PROJECT_NUMBER=${PROJECT_NUMBER})"
echo "    BENCH_BUCKET=gs://${BENCH_BUCKET} (location=${GCS_LOCATION})"
echo "    BQ_LOCATION=${BQ_LOCATION} | BQ_ADMIN_PROJECT_ID=${BQ_ADMIN_PROJECT_ID}"
echo "    DATAPROC_REGION=${DATAPROC_REGION} | DATAPROC_ZONE=${DATAPROC_ZONE} | CLUSTERS=${CLUSTERS_MODE}"
echo "    DATAPROC_SA=${DATAPROC_SA_EMAIL} | MAX_IDLE=${CLUSTER_MAX_IDLE} | MAX_AGE=${CLUSTER_MAX_AGE}"

# ---------------------------------------------------------------------------
# (a) Enable required GCP APIs
# ---------------------------------------------------------------------------
echo "==> [1/6] Enabling required GCP APIs on ${PROJECT_ID}..."
REQUIRED_APIS=(
  "bigquery.googleapis.com"
  "bigqueryreservation.googleapis.com"
  "bigqueryconnection.googleapis.com"
  "bigquerystorage.googleapis.com"
  "biglake.googleapis.com"
  "dataproc.googleapis.com"
  "compute.googleapis.com"
  "storage.googleapis.com"
  "iam.googleapis.com"
  "cloudresourcemanager.googleapis.com"
)
gcloud services enable --project="${PROJECT_ID}" "${REQUIRED_APIS[@]}"

if [[ "${BQ_ADMIN_PROJECT_ID}" != "${PROJECT_ID}" && "${SKIP_RESERVATION}" == "false" && "${REUSE_RESERVATION}" == "false" ]]; then
  if ! gcloud services enable --project="${BQ_ADMIN_PROJECT_ID}" "bigqueryreservation.googleapis.com" >/dev/null 2>&1; then
    echo "    [WARN] Could not enable bigqueryreservation.googleapis.com on ${BQ_ADMIN_PROJECT_ID}; assuming already enabled." >&2
  fi
fi

# ---------------------------------------------------------------------------
# (b) Create GCS bucket
# ---------------------------------------------------------------------------
echo "==> [2/6] Ensuring GCS bucket gs://${BENCH_BUCKET} (${GCS_LOCATION})..."
if gcloud storage buckets describe "gs://${BENCH_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Bucket gs://${BENCH_BUCKET} already exists."
else
  gcloud storage buckets create "gs://${BENCH_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${GCS_LOCATION}" \
    --uniform-bucket-level-access \
    --public-access-prevention \
    --soft-delete-duration=0
  echo "    Created bucket gs://${BENCH_BUCKET}."
fi

# ---------------------------------------------------------------------------
# (c) Create BigLake Cloud Resource connection and grant GCS viewer IAM
# ---------------------------------------------------------------------------
echo "==> [3/6] Ensuring BigLake connection ${BIGLAKE_CONNECTION_ID} (${BQ_LOCATION})..."
if bq show --connection --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BIGLAKE_CONNECTION_ID}" >/dev/null 2>&1; then
  echo "    BigLake connection ${BIGLAKE_CONNECTION_ID} already exists."
else
  bq mk --connection \
    --connection_type=CLOUD_RESOURCE \
    --project_id="${PROJECT_ID}" \
    --location="${BQ_LOCATION}" \
    "${BIGLAKE_CONNECTION_ID}"
  echo "    Created BigLake connection ${BIGLAKE_CONNECTION_ID}."
fi

conn_json="$(bq show --connection --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BIGLAKE_CONNECTION_ID}")"
BIGLAKE_SA="$(python3 - "${conn_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
if idx == -1:
    sys.exit(1)
obj = json.loads(raw[idx:])

sa = (
    (obj.get("cloudResource") or {}).get("serviceAccountId")
    or (obj.get("cloud_resource") or {}).get("service_account_id")
    or ""
)
if not sa and "properties" in obj:
    props = obj["properties"]
    if isinstance(props, str):
        try:
            props = json.loads(props)
        except json.JSONDecodeError:
            props = {}
    if isinstance(props, dict):
        sa = props.get("serviceAccountId") or props.get("service_account_id") or ""

if not sa:
    sys.exit(1)
print(sa)
PY
)" || {
  echo "ERROR: Unable to extract BigLake serviceAccountId from connection '${BIGLAKE_CONNECTION_ID}'." >&2
  exit 1
}

echo "    Granting roles/storage.objectViewer on gs://${BENCH_BUCKET} to ${BIGLAKE_SA}..."
gcloud storage buckets add-iam-policy-binding "gs://${BENCH_BUCKET}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${BIGLAKE_SA}" \
  --role="roles/storage.objectViewer" \
  --condition=None >/dev/null

# ---------------------------------------------------------------------------
# (d) Create BigQuery datasets, configure PHYSICAL billing, and create metrics table
# ---------------------------------------------------------------------------
echo "==> [4/6] Ensuring BigQuery datasets and ${DS_METRICS}.benchmark_runs table..."
ALL_DATASETS=(
  "${DS_BRONZE_BIGLAKE}"
  "${DS_BRONZE_NATIVE}"
  "${DS_SILVER_BQ}"
  "${DS_GOLD_BQ}"
  "${DS_METRICS}"
)

for ds in "${ALL_DATASETS[@]}"; do
  if bq show --project_id="${PROJECT_ID}" --format=none "${PROJECT_ID}:${ds}" >/dev/null 2>&1; then
    echo "    Dataset ${PROJECT_ID}:${ds} already exists."
  else
    bq mk --dataset --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" "${PROJECT_ID}:${ds}"
    echo "    Created dataset ${PROJECT_ID}:${ds}."
  fi
done

PHYSICAL_DATASETS=(
  "${DS_BRONZE_NATIVE}"
  "${DS_SILVER_BQ}"
  "${DS_GOLD_BQ}"
)

for ds in "${PHYSICAL_DATASETS[@]}"; do
  current_model="$(bq query --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --use_legacy_sql=false --format=csv --quiet <<SQL 2>/dev/null | grep -Eo 'PHYSICAL|LOGICAL' | tail -n 1 || true
SELECT option_value
FROM \`${PROJECT_ID}.region-${BQ_LOCATION_LOWER}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS\`
WHERE schema_name = '${ds}'
  AND option_name = 'storage_billing_model'
SQL
)"
  if [[ "${current_model}" == "PHYSICAL" ]]; then
    echo "    Dataset ${ds} already uses storage_billing_model='PHYSICAL'."
  else
    if alter_out="$(bq query --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --use_legacy_sql=false --quiet 2>&1 <<SQL
ALTER SCHEMA \`${PROJECT_ID}.${ds}\`
SET OPTIONS(storage_billing_model='PHYSICAL');
SQL
)"; then
      echo "    Configured storage_billing_model='PHYSICAL' on ${ds}."
    else
      if [[ "${alter_out}" == *"PHYSICAL"* || "${alter_out}" == *"14 days"* ]]; then
        echo "    [WARN] Dataset ${ds} storage_billing_model update skipped: ${alter_out}" >&2
      else
        echo "ERROR: Failed to configure storage_billing_model='PHYSICAL' on ${ds}: ${alter_out}" >&2
        exit 1
      fi
    fi
  fi
done

bq query --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --use_legacy_sql=false --quiet <<SQL
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DS_METRICS}.benchmark_runs\` (
  run_id STRING NOT NULL,
  timestamp_utc TIMESTAMP NOT NULL,
  scale_preset STRING NOT NULL,
  engine_variant STRING NOT NULL,
  storage_backend STRING NOT NULL,
  stage_id STRING NOT NULL,
  start_epoch_sec FLOAT64 NOT NULL,
  end_epoch_sec FLOAT64 NOT NULL,
  wall_clock_sec FLOAT64 NOT NULL,
  output_row_count INT64 NOT NULL,
  checksum_net_paid FLOAT64,
  worker_machine_type STRING,
  total_worker_vcpus INT64,
  vcpu_seconds FLOAT64,
  bq_slot_cap INT64,
  bq_total_slot_ms INT64,
  bq_bytes_processed INT64,
  nqe_enabled BOOL NOT NULL,
  nqe_fallback_note STRING,
  est_list_cost_usd FLOAT64,
  sample_fraction FLOAT64
);
SQL
echo "    Ensured table ${PROJECT_ID}.${DS_METRICS}.benchmark_runs."

# ---------------------------------------------------------------------------
# (e) BigQuery Enterprise Edition slot reservation & QUERY assignment
# ---------------------------------------------------------------------------
inspect_query_assignment() {
  local target_res="$1"
  local assignments_json
  assignments_json="$(bq ls --reservation_assignment --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json 2>/dev/null || true)"
  python3 - "${assignments_json}" "${PROJECT_ID}" "${PROJECT_NUMBER}" "${target_res}" <<'PY'
import json
import sys

raw, project_id, project_number, target_res = sys.argv[1:5]
idx = raw.find("[")
if idx == -1:
    print("NONE")
    sys.exit(0)

try:
    items = json.loads(raw[idx:])
except json.JSONDecodeError:
    print("NONE")
    sys.exit(0)

targets = {f"projects/{project_id}", f"projects/{project_number}"}
for item in items:
    job_type = item.get("jobType") or item.get("job_type") or ""
    assignee = item.get("assignee") or ""
    if job_type == "QUERY" and assignee in targets:
        name = item.get("name") or ""
        res_name = ""
        if "/reservations/" in name:
            res_name = name.split("/reservations/", 1)[1].split("/", 1)[0]
        elif "." in name:
            parts = name.split(":", 1)[-1].split(".")
            if len(parts) >= 2:
                res_name = parts[-2]
        if res_name == target_res:
            print(f"MATCH:{name}")
        else:
            print(f"CONFLICT:{name}")
        sys.exit(0)

print("NONE")
PY
}

if [[ "${SKIP_RESERVATION}" == "true" ]]; then
  echo "==> [5/6] Skipping BigQuery slot reservation (--skip-reservation set)."
elif [[ "${REUSE_RESERVATION}" == "true" ]]; then
  echo "==> [5/6] Verifying existing BigQuery reservation '${REUSE_RESERVATION_NAME}' in ${BQ_LOCATION} (--reuse-reservation, read-only)..."
  if ! existing_res_json="$(bq show --reservation --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${REUSE_RESERVATION_NAME}" 2>/dev/null)"; then
    echo "ERROR: Reservation '${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${REUSE_RESERVATION_NAME}' not found." >&2
    echo "       --reuse-reservation never creates, resizes, or patches reservations." >&2
    exit 1
  fi
  res_summary="$(python3 - "${existing_res_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
if idx == -1:
    print("edition=UNKNOWN baseline=0 autoscale_max=0")
    sys.exit(0)
obj = json.loads(raw[idx:])
edition = obj.get("edition") or "UNKNOWN"
cur_base = int(obj.get("slotCapacity") or 0)
cur_auto = int(((obj.get("autoscale") or {}).get("maxSlots")) or 0)
print(f"edition={edition} baseline={cur_base} autoscale_max={cur_auto}")
PY
)"
  echo "    Verified reservation ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${REUSE_RESERVATION_NAME} (${res_summary}) — left untouched."

  assignment_state="$(inspect_query_assignment "${REUSE_RESERVATION_NAME}")"
  case "${assignment_state}" in
    MATCH:*)
      echo "    Verified existing QUERY assignment for projects/${PROJECT_ID} -> ${REUSE_RESERVATION_NAME} (${assignment_state#MATCH:})."
      ;;
    CONFLICT:*)
      echo "ERROR: Project '${PROJECT_ID}' has a QUERY assignment to a different reservation: ${assignment_state#CONFLICT:}" >&2
      exit 1
      ;;
    NONE)
      echo "ERROR: No QUERY reservation assignment found for projects/${PROJECT_ID} pointing to '${REUSE_RESERVATION_NAME}' in '${BQ_LOCATION}'." >&2
      echo "       Assign it first or omit --reuse-reservation if provisioning a dedicated benchmark reservation." >&2
      exit 1
      ;;
    *)
      echo "ERROR: Unexpected assignment inspection result: ${assignment_state}" >&2
      exit 1
      ;;
  esac
else
  if [[ ! "${BQ_BASELINE_SLOTS}" =~ ^[0-9]+$ || ! "${BQ_SLOT_CAP}" =~ ^[0-9]+$ || "${BQ_SLOT_CAP}" -le 0 ]]; then
    echo "ERROR: Invalid BQ_BASELINE_SLOTS='${BQ_BASELINE_SLOTS}' or BQ_SLOT_CAP='${BQ_SLOT_CAP}'." >&2
    exit 1
  fi
  if [[ "${BQ_BASELINE_SLOTS}" -gt "${BQ_SLOT_CAP}" ]]; then
    echo "ERROR: BQ_BASELINE_SLOTS (${BQ_BASELINE_SLOTS}) cannot exceed BQ_SLOT_CAP (${BQ_SLOT_CAP})." >&2
    exit 1
  fi

  AUTOSCALE_SLOTS=$(( BQ_SLOT_CAP - BQ_BASELINE_SLOTS ))

  echo "==> [5/6] Configuring BigQuery Enterprise reservation ${BQ_RESERVATION_NAME} (baseline=${BQ_BASELINE_SLOTS}, autoscale=${AUTOSCALE_SLOTS}, max_slots=${BQ_SLOT_CAP})..."
  if existing_res_json="$(bq show --reservation --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BQ_RESERVATION_NAME}" 2>/dev/null)"; then
    res_needs_update="$(python3 - "${existing_res_json}" "${BQ_BASELINE_SLOTS}" "${AUTOSCALE_SLOTS}" <<'PY'
import json
import sys

raw, want_base, want_auto = sys.argv[1:4]
idx = raw.find("{")
if idx == -1:
    print("0")
    sys.exit(0)
obj = json.loads(raw[idx:])
cur_base = int(obj.get("slotCapacity") or 0)
cur_auto = int(((obj.get("autoscale") or {}).get("maxSlots")) or 0)
print("1" if (cur_base != int(want_base) or cur_auto != int(want_auto)) else "0")
PY
)"
    if [[ "${res_needs_update}" == "1" ]]; then
      echo "    Updating reservation ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME} to baseline=${BQ_BASELINE_SLOTS}, autoscale_max_slots=${AUTOSCALE_SLOTS}..."
      bq update --reservation \
        --project_id="${BQ_ADMIN_PROJECT_ID}" \
        --location="${BQ_LOCATION}" \
        --slots="${BQ_BASELINE_SLOTS}" \
        --autoscale_max_slots="${AUTOSCALE_SLOTS}" \
        "${BQ_RESERVATION_NAME}"
    else
      echo "    Reservation ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME} already matches desired capacity."
    fi
  else
    mk_res_cmd=(
      bq mk --reservation
      --project_id="${BQ_ADMIN_PROJECT_ID}"
      --location="${BQ_LOCATION}"
      --edition=ENTERPRISE
      --slots="${BQ_BASELINE_SLOTS}"
    )
    if [[ "${AUTOSCALE_SLOTS}" -gt 0 ]]; then
      mk_res_cmd+=(--autoscale_max_slots="${AUTOSCALE_SLOTS}")
    fi
    mk_res_cmd+=("${BQ_RESERVATION_NAME}")
    if ! "${mk_res_cmd[@]}"; then
      echo "ERROR: Failed to create reservation '${BQ_RESERVATION_NAME}'. If this sandbox lacks reservation admin quota or already hosts another reservation, re-run with --reuse-reservation=<name> or --skip-reservation." >&2
      exit 1
    fi
    echo "    Created Enterprise reservation ${BQ_RESERVATION_NAME}."
  fi

  assignment_state="$(inspect_query_assignment "${BQ_RESERVATION_NAME}")"
  case "${assignment_state}" in
    MATCH:*)
      echo "    QUERY assignment already points to ${BQ_RESERVATION_NAME} (${assignment_state#MATCH:})."
      ;;
    CONFLICT:*)
      echo "ERROR: Project '${PROJECT_ID}' already has a QUERY assignment to another reservation: ${assignment_state#CONFLICT:}" >&2
      echo "       Re-run with --reuse-reservation=<existing_name> or --skip-reservation so existing reservations stay untouched." >&2
      exit 1
      ;;
    NONE)
      bq mk --reservation_assignment \
        --project_id="${BQ_ADMIN_PROJECT_ID}" \
        --location="${BQ_LOCATION}" \
        --reservation_id="${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME}" \
        --job_type=QUERY \
        --assignee_type=PROJECT \
        --assignee_id="${PROJECT_ID}"
      echo "    Assigned QUERY jobs for projects/${PROJECT_ID} to ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME}."
      ;;
    *)
      echo "ERROR: Unexpected assignment inspection result: ${assignment_state}" >&2
      exit 1
      ;;
  esac
fi

# ---------------------------------------------------------------------------
# (f) Dedicated Dataproc SA, VPC/Subnet Private Google Access, quota preflight & clusters
# ---------------------------------------------------------------------------
echo "==> [6/6] Configuring Dataproc SA, VPC networking, quota preflight, and x86 clusters..."

sa_domain_project="${DATAPROC_SA_EMAIL#*@}"
sa_domain_project="${sa_domain_project%.iam.gserviceaccount.com}"
if gcloud iam service-accounts describe "${DATAPROC_SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Dataproc service account ${DATAPROC_SA_EMAIL} already exists."
elif [[ "${sa_domain_project}" == "${PROJECT_ID}" && "${DATAPROC_SA_EMAIL}" == *.iam.gserviceaccount.com ]]; then
  sa_short_name="${DATAPROC_SA_EMAIL%%@*}"
  echo "    Creating dedicated least-privilege Dataproc SA ${DATAPROC_SA_EMAIL}..."
  gcloud iam service-accounts create "${sa_short_name}" \
    --project="${PROJECT_ID}" \
    --display-name="Lakehouse Benchmark Dataproc Runner"
  sleep 2
else
  echo "    Using pre-existing external/cross-project SA ${DATAPROC_SA_EMAIL}."
fi

echo "    Granting roles/dataproc.worker on ${PROJECT_ID} to ${DATAPROC_SA_EMAIL}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
  --role="roles/dataproc.worker" \
  --condition=None >/dev/null

echo "    Granting roles/storage.objectAdmin on gs://${BENCH_BUCKET} to ${DATAPROC_SA_EMAIL}..."
gcloud storage buckets add-iam-policy-binding "gs://${BENCH_BUCKET}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
  --role="roles/storage.objectAdmin" \
  --condition=None >/dev/null

echo "    Granting BigQuery dataEditor (WRITER) on ${PROJECT_ID}:${DS_METRICS} to ${DATAPROC_SA_EMAIL}..."
if ! ds_metrics_json="$(bq show --project_id="${PROJECT_ID}" --format=json "${PROJECT_ID}:${DS_METRICS}")"; then
  echo "ERROR: Failed to inspect BigQuery dataset ${PROJECT_ID}:${DS_METRICS} before updating ACL." >&2
  exit 1
fi
metrics_acl_tmp="$(mktemp)"
if ! metrics_acl_action="$(python3 - "${ds_metrics_json}" "${DATAPROC_SA_EMAIL}" "${metrics_acl_tmp}" <<'PY'
import json
import sys

raw, sa_email, out_path = sys.argv[1:4]
idx = raw.find("{")
if idx == -1:
    raise SystemExit(f"ERROR: Expected JSON from bq show on DS_METRICS, got: {raw[:200]!r}")
obj = json.loads(raw[idx:])
access = list(obj.get("access") or [])
if not access:
    raise SystemExit("ERROR: Refusing to update DS_METRICS ACL because existing access list is empty.")
writer_roles = {"WRITER", "roles/bigquery.dataEditor", "OWNER", "roles/bigquery.dataOwner"}
already_present = any(
    (e.get("userByEmail") == sa_email or e.get("iamMember") == f"serviceAccount:{sa_email}")
    and e.get("role") in writer_roles
    for e in access
    if isinstance(e, dict)
)
if already_present:
    print("UNCHANGED")
    sys.exit(0)
access.append({"role": "WRITER", "userByEmail": sa_email})
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump({"access": access}, fh)
print("UPDATE")
PY
)"; then
  rm -f "${metrics_acl_tmp}"
  exit 1
fi
if [[ "${metrics_acl_action}" == "UPDATE" ]]; then
  bq update --project_id="${PROJECT_ID}" --source="${metrics_acl_tmp}" "${PROJECT_ID}:${DS_METRICS}" >/dev/null
fi
rm -f "${metrics_acl_tmp}"

# Resolve VPC & subnet without touching 'default' unless --subnet=default is explicitly passed
DEFAULT_BENCH_SUBNET="bench-pga-${DATAPROC_REGION}"
TARGET_SUBNET="${BENCH_SUBNET:-${DEFAULT_BENCH_SUBNET}}"

ensure_internal_firewall() {
  local net_name="$1"
  local cidr_range="$2"
  local fw_json
  fw_json="$(gcloud compute firewall-rules list --project="${PROJECT_ID}" --format=json)"
  local has_internal_fw
  has_internal_fw="$(python3 - "${fw_json}" "${net_name}" <<'PY'
import json
import sys

raw, net_name = sys.argv[1:3]
idx = raw.find("[")
rules = json.loads(raw[idx:]) if idx != -1 else []
for rule in rules:
    if rule.get("disabled"):
        continue
    if rule.get("direction") != "INGRESS":
        continue
    rule_net = (rule.get("network") or "").rstrip("/").split("/")[-1]
    if rule_net != net_name:
        continue
    name = rule.get("name", "")
    if name in ("default-allow-internal", "bench-allow-internal", "sandbox-allow-internal") or name.endswith("-allow-internal"):
        print("YES")
        sys.exit(0)
print("NO")
PY
)"
  if [[ "${has_internal_fw}" != "YES" ]]; then
    if [[ -z "${cidr_range}" ]]; then
      echo "ERROR: Unable to determine subnet CIDR for firewall creation on '${net_name}'." >&2
      exit 1
    fi
    local fw_name="bench-allow-internal"
    if [[ "${net_name}" != "bench-vpc" && "${net_name}" != "default" ]]; then
      fw_name="${net_name}-allow-internal"
    fi
    echo "    Creating internal Dataproc firewall rule '${fw_name}' on VPC '${net_name}' (${cidr_range})..."
    gcloud compute firewall-rules create "${fw_name}" \
      --project="${PROJECT_ID}" \
      --network="${net_name}" \
      --direction=INGRESS \
      --action=ALLOW \
      --rules="tcp:0-65535,udp:0-65535,icmp" \
      --source-ranges="${cidr_range}"
  else
    echo "    Internal VPC firewall rule already present on network '${net_name}'."
  fi
}

inspect_subnet_json() {
  local raw_json="$1"
  python3 - "${raw_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
obj = json.loads(raw[idx:])
pga = "true" if obj.get("privateIpGoogleAccess") else "false"
cidr = obj.get("ipCidrRange", "")
network_url = obj.get("network", "")
network_name = network_url.rstrip("/").split("/")[-1] if network_url else ""
print(f"{pga} {cidr} {network_name}")
PY
}

if [[ "${TARGET_SUBNET}" == "default" ]]; then
  if ! subnet_json="$(gcloud compute networks subnets describe default --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" --format=json 2>/dev/null)"; then
    echo "ERROR: Explicit --subnet=default requested, but 'default' subnet does not exist in ${DATAPROC_REGION}." >&2
    echo "       Use --create-subnet (or omit --subnet to auto-create bench-vpc / ${DEFAULT_BENCH_SUBNET})." >&2
    exit 1
  fi
  read -r PRIVATE_PGA SUBNET_CIDR NETWORK_NAME <<<"$(inspect_subnet_json "${subnet_json}")"
  if [[ "${PRIVATE_PGA}" != "true" ]]; then
    echo "    Enabling privateIpGoogleAccess on explicitly requested subnet 'default' (${DATAPROC_REGION})..."
    gcloud compute networks subnets update default \
      --region="${DATAPROC_REGION}" \
      --project="${PROJECT_ID}" \
      --enable-private-ip-google-access
  else
    echo "    Subnet 'default' (${DATAPROC_REGION}) already has privateIpGoogleAccess enabled."
  fi
  ensure_internal_firewall "${NETWORK_NAME}" "${SUBNET_CIDR}"
else
  if subnet_json="$(gcloud compute networks subnets describe "${TARGET_SUBNET}" --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" --format=json 2>/dev/null)"; then
    read -r PRIVATE_PGA SUBNET_CIDR NETWORK_NAME <<<"$(inspect_subnet_json "${subnet_json}")"
    if [[ -n "${BENCH_NETWORK_ARG}" && "${NETWORK_NAME}" != "${BENCH_NETWORK_ARG}" ]]; then
      echo "ERROR: Subnet '${TARGET_SUBNET}' belongs to VPC '${NETWORK_NAME}', not requested --network='${BENCH_NETWORK_ARG}'." >&2
      exit 1
    fi
    echo "    Reusing existing subnet '${TARGET_SUBNET}' on VPC '${NETWORK_NAME}' (${DATAPROC_REGION}, CIDR=${SUBNET_CIDR}, PGA=${PRIVATE_PGA})."
    if [[ "${PRIVATE_PGA}" != "true" ]]; then
      echo "    Enabling privateIpGoogleAccess on subnet '${TARGET_SUBNET}' (${DATAPROC_REGION})..."
      gcloud compute networks subnets update "${TARGET_SUBNET}" \
        --region="${DATAPROC_REGION}" \
        --project="${PROJECT_ID}" \
        --enable-private-ip-google-access
    fi
    ensure_internal_firewall "${NETWORK_NAME}" "${SUBNET_CIDR}"
  else
    if [[ -n "${BENCH_SUBNET}" && "${CREATE_SUBNET}" != "true" ]]; then
      echo "ERROR: Subnet '${TARGET_SUBNET}' not found in ${DATAPROC_REGION}." >&2
      echo "       Pass --create-subnet to create '${TARGET_SUBNET}' on VPC '${BENCH_NETWORK}' (${BENCH_SUBNET_CIDR})," >&2
      echo "       or specify an existing subnet (for example from Repo A) via --subnet=<name>." >&2
      exit 1
    fi

    if [[ "${BENCH_NETWORK}" == "default" ]]; then
      echo "ERROR: Refusing to create custom subnet '${TARGET_SUBNET}' inside the 'default' VPC." >&2
      echo "       Pass --network=bench-vpc (or another custom VPC name)." >&2
      exit 1
    fi

    if gcloud compute networks describe "${BENCH_NETWORK}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
      echo "    Using existing custom VPC '${BENCH_NETWORK}'."
    else
      echo "    Creating custom VPC '${BENCH_NETWORK}' (subnet-mode=custom, no Cloud NAT)..."
      gcloud compute networks create "${BENCH_NETWORK}" \
        --project="${PROJECT_ID}" \
        --subnet-mode=custom
    fi

    echo "    Creating Private Google Access subnet '${TARGET_SUBNET}' (${BENCH_SUBNET_CIDR} in ${DATAPROC_REGION}, no Cloud NAT)..."
    gcloud compute networks subnets create "${TARGET_SUBNET}" \
      --project="${PROJECT_ID}" \
      --network="${BENCH_NETWORK}" \
      --region="${DATAPROC_REGION}" \
      --range="${BENCH_SUBNET_CIDR}" \
      --enable-private-ip-google-access

    subnet_json="$(gcloud compute networks subnets describe "${TARGET_SUBNET}" --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" --format=json)"
    read -r PRIVATE_PGA SUBNET_CIDR NETWORK_NAME <<<"$(inspect_subnet_json "${subnet_json}")"
    ensure_internal_firewall "${NETWORK_NAME}" "${SUBNET_CIDR}"
  fi
fi

cluster_exists() {
  local cluster_name="$1"
  gcloud dataproc clusters describe "${cluster_name}" --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1
}

check_regional_vcpu_quota() {
  local clusters_to_create="$1"
  if [[ "${clusters_to_create}" -le 0 ]]; then
    return 0
  fi

  local cluster_vcpus
  cluster_vcpus="$(python3 - "${MASTER_MACHINE_TYPE}" "${WORKER_MACHINE_TYPE}" "${NUM_PRIMARY_WORKERS}" <<'PY'
import re
import sys

def infer(mtype: str) -> int:
    m = re.match(r"^(?:[a-z0-9]+-)?custom-(\d+)-\d+$", mtype.strip())
    if m:
        return int(m.group(1))
    m2 = re.search(r"-(\d+)$", mtype.strip())
    return int(m2.group(1)) if m2 else 8

master_v = infer(sys.argv[1])
worker_v = infer(sys.argv[2])
workers = int(sys.argv[3])
print(master_v + worker_v * workers)
PY
)"
  local needed_vcpus=$(( cluster_vcpus * clusters_to_create ))

  local region_json
  if ! region_json="$(gcloud compute regions describe "${DATAPROC_REGION}" --project="${PROJECT_ID}" --format=json 2>/dev/null)"; then
    echo "    [WARN] Unable to inspect regional compute quotas in ${DATAPROC_REGION}; skipping vCPU preflight." >&2
    return 0
  fi

  local quota_report
  quota_report="$(python3 - "${region_json}" "${WORKER_MACHINE_TYPE}" "${needed_vcpus}" <<'PY'
import json
import sys

raw, worker_type, needed_str = sys.argv[1:4]
needed = int(needed_str)
idx = raw.find("{")
if idx == -1:
    print("OK|0|0|CPUS")
    sys.exit(0)
data = json.loads(raw[idx:])
quotas = {q.get("metric"): q for q in (data.get("quotas") or []) if q.get("metric")}

family = worker_type.strip().split("-")[0].upper() if worker_type else "N2D"
family_metric = f"{family}_CPUS" if f"{family}_CPUS" in quotas else "N2D_CPUS"

def avail(metric_name: str):
    q = quotas.get(metric_name)
    if not q:
        return None
    limit = float(q.get("limit", 0.0))
    usage = float(q.get("usage", 0.0))
    return max(0.0, limit - usage), limit, usage

fam_info = avail(family_metric)
cpu_info = avail("CPUS")

bottleneck_metric = "CPUS"
bottleneck_avail = float("inf")
details = []
for name, info in ((family_metric, fam_info), ("CPUS", cpu_info)):
    if info is None:
        continue
    av, lim, usg = info
    details.append(f"{name}={av:.0f}/{lim:.0f} free (used {usg:.0f})")
    if av < bottleneck_avail:
        bottleneck_avail = av
        bottleneck_metric = name

if bottleneck_avail == float("inf"):
    print("OK|0|0|NONE")
    sys.exit(0)

status = "LOW" if bottleneck_avail < needed else "OK"
print(f"{status}|{int(bottleneck_avail)}|{needed}|{bottleneck_metric}|" + ", ".join(details))
PY
)"

  IFS='|' read -r q_status q_avail q_needed q_metric q_details <<<"${quota_report}"
  if [[ "${q_details:-}" != "" ]]; then
    echo "    Regional quota check (${DATAPROC_REGION}): ${q_details} (need ${q_needed} vCPUs for ${clusters_to_create} pending cluster(s))."
  fi
  if [[ "${q_status}" == "LOW" ]]; then
    echo "" >&2
    echo "ERROR: Insufficient regional ${q_metric} quota in '${DATAPROC_REGION}': ${q_avail} vCPUs available, need ${q_needed} vCPUs." >&2
    if [[ "${CLUSTERS_MODE}" == "both" && "${q_avail}" -ge "${cluster_vcpus}" ]]; then
      echo "       Simultaneous --clusters=both requires $(( cluster_vcpus * 2 )) vCPUs (2 x ${cluster_vcpus}: 1x${MASTER_MACHINE_TYPE} + ${NUM_PRIMARY_WORKERS}x${WORKER_MACHINE_TYPE} each)." >&2
      echo "       Run in sequential ${cluster_vcpus}-vCPU mode instead:" >&2
      echo "         1) scripts/provision_infra.sh --clusters=std" >&2
      echo "         2) Run notebooks/01_medallion_spark_standard.ipynb" >&2
      echo "         3) scripts/teardown_infra.sh --clusters=std --keep-reservation --confirm" >&2
      echo "         4) scripts/provision_infra.sh --clusters=lightning" >&2
      echo "         5) Run notebooks/02_medallion_spark_lightning.ipynb" >&2
    else
      echo "       Free existing GCE/Dataproc instances or request higher ${q_metric}/CPUS quota in ${DATAPROC_REGION}." >&2
    fi
    exit 1
  fi
}

PENDING_CLUSTERS=0
case "${CLUSTERS_MODE}" in
  both)
    cluster_exists "${CLUSTER_STD_NAME}" || PENDING_CLUSTERS=$(( PENDING_CLUSTERS + 1 ))
    cluster_exists "${CLUSTER_LGT_NAME}" || PENDING_CLUSTERS=$(( PENDING_CLUSTERS + 1 ))
    ;;
  std)
    cluster_exists "${CLUSTER_STD_NAME}" || PENDING_CLUSTERS=$(( PENDING_CLUSTERS + 1 ))
    ;;
  lightning)
    cluster_exists "${CLUSTER_LGT_NAME}" || PENDING_CLUSTERS=$(( PENDING_CLUSTERS + 1 ))
    ;;
  none)
    PENDING_CLUSTERS=0
    ;;
esac

if [[ "${CLUSTERS_MODE}" != "none" ]]; then
  check_regional_vcpu_quota "${PENDING_CLUSTERS}"
fi

create_dataproc_cluster() {
  local cluster_name="$1"
  local engine_mode="$2"

  if cluster_exists "${cluster_name}"; then
    echo "    Dataproc cluster '${cluster_name}' already exists."
    return 0
  fi

  local -a cmd=(
    gcloud dataproc clusters create "${cluster_name}"
    --project="${PROJECT_ID}"
    --region="${DATAPROC_REGION}"
    --zone="${DATAPROC_ZONE}"
    --engine="${engine_mode}"
    --image-version="${DATAPROC_IMAGE_VERSION}"
    --master-machine-type="${MASTER_MACHINE_TYPE}"
    --worker-machine-type="${WORKER_MACHINE_TYPE}"
    --num-workers="${NUM_PRIMARY_WORKERS}"
    --num-worker-local-ssds="${NUM_WORKER_LOCAL_SSDS}"
    --worker-local-ssd-interface=NVME
    --subnet="${TARGET_SUBNET}"
    --no-address
    --shielded-secure-boot
    --service-account="${DATAPROC_SA_EMAIL}"
    --max-idle="${CLUSTER_MAX_IDLE}"
    --max-age="${CLUSTER_MAX_AGE}"
    --bucket="${BENCH_BUCKET}"
  )

  echo "    Creating Dataproc cluster '${cluster_name}' (--engine=${engine_mode}, subnet=${TARGET_SUBNET}, sa=${DATAPROC_SA_EMAIL}, ${NUM_PRIMARY_WORKERS}x ${WORKER_MACHINE_TYPE}, ${NUM_WORKER_LOCAL_SSDS}x NVMe Local SSD, max-idle=${CLUSTER_MAX_IDLE}, max-age=${CLUSTER_MAX_AGE})..."
  "${cmd[@]}"
}

case "${CLUSTERS_MODE}" in
  both)
    create_dataproc_cluster "${CLUSTER_STD_NAME}" "default"
    create_dataproc_cluster "${CLUSTER_LGT_NAME}" "lightning"
    ;;
  std)
    create_dataproc_cluster "${CLUSTER_STD_NAME}" "default"
    ;;
  lightning)
    create_dataproc_cluster "${CLUSTER_LGT_NAME}" "lightning"
    ;;
  none)
    echo "    Skipping Dataproc cluster creation (--clusters=none)."
    ;;
esac

echo ""
echo "==> Provisioning complete."
echo "    Bucket:      gs://${BENCH_BUCKET}"
echo "    BigLake:     ${PROJECT_ID}.${BQ_LOCATION_LOWER}.${BIGLAKE_CONNECTION_ID} (SA: ${BIGLAKE_SA})"
echo "    Datasets:    ${DS_BRONZE_BIGLAKE}, ${DS_BRONZE_NATIVE}, ${DS_SILVER_BQ}, ${DS_GOLD_BQ}, ${DS_METRICS}"
if [[ "${SKIP_RESERVATION}" == "false" ]]; then
  echo "    Reservation: ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME}"
fi
echo "    Network:     vpc=${NETWORK_NAME} subnet=${TARGET_SUBNET} (PGA ON, no NAT)"
echo "    Dataproc SA: ${DATAPROC_SA_EMAIL}"
if [[ "${CLUSTERS_MODE}" != "none" ]]; then
  echo "    Clusters:    mode=${CLUSTERS_MODE} (${CLUSTER_STD_NAME}, ${CLUSTER_LGT_NAME} in ${DATAPROC_REGION})"
fi
