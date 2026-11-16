#!/usr/bin/env bash
set -euo pipefail

# Provision GCP infrastructure for the Lakehouse Compute Benchmark:
#   - Required GCP APIs
#   - GCS bucket (US multi-region, UBLA, PAP) + BigLake Cloud Resource connection + IAM
#   - 5 BigQuery datasets (PHYSICAL storage billing on native/silver/gold) + metrics table
#   - BigQuery Enterprise Edition autoscale slot reservation + QUERY assignment
#   - Subnet Private Google Access + internal firewall rule + Dataproc GCE x86 clusters

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

CLUSTERS_MODE="both"
SKIP_RESERVATION="false"

usage() {
  cat <<'EOF'
Usage: scripts/provision_infra.sh [OPTIONS]

Idempotently provisions GCP resources configured in .env (or environment variables).

Options:
  --clusters=<both|std|lightning|none>   Which Dataproc GCE x86 clusters to create (default: both)
  --skip-reservation                     Skip BigQuery Enterprise slot reservation & assignment
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
GCS_LOCATION="${GCS_LOCATION:-US}"
DATAPROC_REGION="${DATAPROC_REGION:-us-central1}"
DATAPROC_ZONE="${DATAPROC_ZONE:-us-central1-a}"
DATAPROC_IMAGE_VERSION="${DATAPROC_IMAGE_VERSION:-2.3-debian12}"
CLUSTER_STD_NAME="${CLUSTER_STD_NAME:-bench-spark-std-x86}"
CLUSTER_LGT_NAME="${CLUSTER_LGT_NAME:-bench-spark-lightning-x86}"
MASTER_MACHINE_TYPE="${MASTER_MACHINE_TYPE:-n2d-standard-4}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n2d-standard-8}"
NUM_PRIMARY_WORKERS="${NUM_PRIMARY_WORKERS:-2}"
NUM_WORKER_LOCAL_SSDS="${NUM_WORKER_LOCAL_SSDS:-1}"
BQ_RESERVATION_NAME="${BQ_RESERVATION_NAME:-bench-elt-slots}"
BQ_BASELINE_SLOTS="${BQ_BASELINE_SLOTS:-0}"
BQ_SLOT_CAP="${BQ_SLOT_CAP:-100}"
BIGLAKE_CONNECTION_ID="${BIGLAKE_CONNECTION_ID:-bench-biglake-us}"
DS_BRONZE_NATIVE="${DS_BRONZE_NATIVE:-bench_bronze_native}"
DS_BRONZE_BIGLAKE="${DS_BRONZE_BIGLAKE:-bench_bronze_biglake}"
DS_SILVER_BQ="${DS_SILVER_BQ:-bench_silver_bq}"
DS_GOLD_BQ="${DS_GOLD_BQ:-bench_gold_bq}"
DS_METRICS="${DS_METRICS:-bench_metrics}"

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
BQ_LOCATION_LOWER="$(printf '%s' "${BQ_LOCATION}" | tr '[:upper:]' '[:lower:]')"
DATAPROC_SA="${DATAPROC_SERVICE_ACCOUNT:-${PROJECT_NUMBER}-compute@developer.gserviceaccount.com}"

echo "==> Provisioning benchmark infrastructure"
echo "    PROJECT_ID=${PROJECT_ID} (PROJECT_NUMBER=${PROJECT_NUMBER})"
echo "    BENCH_BUCKET=gs://${BENCH_BUCKET} (location=${GCS_LOCATION})"
echo "    BQ_LOCATION=${BQ_LOCATION} | BQ_ADMIN_PROJECT_ID=${BQ_ADMIN_PROJECT_ID}"
echo "    DATAPROC_REGION=${DATAPROC_REGION} | DATAPROC_ZONE=${DATAPROC_ZONE} | CLUSTERS=${CLUSTERS_MODE}"

# ---------------------------------------------------------------------------
# (a) Enable required GCP APIs
# ---------------------------------------------------------------------------
echo "==> [1/6] Enabling required GCP APIs on ${PROJECT_ID}..."
REQUIRED_APIS=(
  "bigquery.googleapis.com"
  "bigqueryreservation.googleapis.com"
  "bigqueryconnection.googleapis.com"
  "biglake.googleapis.com"
  "dataproc.googleapis.com"
  "compute.googleapis.com"
  "storage.googleapis.com"
)
gcloud services enable --project="${PROJECT_ID}" "${REQUIRED_APIS[@]}"

if [[ "${BQ_ADMIN_PROJECT_ID}" != "${PROJECT_ID}" && "${SKIP_RESERVATION}" == "false" ]]; then
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
    --public-access-prevention
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
  --role="roles/storage.objectViewer" >/dev/null

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
if [[ "${SKIP_RESERVATION}" == "true" ]]; then
  echo "==> [5/6] Skipping BigQuery slot reservation (--skip-reservation set)."
else
  if [[ ! "${BQ_BASELINE_SLOTS}" =~ ^[0-9]+$ || ! "${BQ_SLOT_CAP}" =~ ^[0-9]+$ || "${BQ_SLOT_CAP}" -le 0 ]]; then
    echo "ERROR: Invalid BQ_BASELINE_SLOTS='${BQ_BASELINE_SLOTS}' or BQ_SLOT_CAP='${BQ_SLOT_CAP}'." >&2
    exit 1
  fi
  if [[ "${BQ_BASELINE_SLOTS}" -gt "${BQ_SLOT_CAP}" ]]; then
    echo "ERROR: BQ_BASELINE_SLOTS (${BQ_BASELINE_SLOTS}) cannot exceed BQ_SLOT_CAP (${BQ_SLOT_CAP})." >&2
    exit 1
  fi

  echo "==> [5/6] Configuring BigQuery Enterprise reservation ${BQ_RESERVATION_NAME} (baseline=${BQ_BASELINE_SLOTS}, max_slots=${BQ_SLOT_CAP})..."
  if existing_res_json="$(bq show --reservation --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json "${BQ_RESERVATION_NAME}" 2>/dev/null)"; then
    desired_autoscale=0
    if [[ "${BQ_BASELINE_SLOTS}" -lt "${BQ_SLOT_CAP}" ]]; then
      desired_autoscale="${BQ_SLOT_CAP}"
    fi
    res_needs_update="$(python3 - "${existing_res_json}" "${BQ_BASELINE_SLOTS}" "${desired_autoscale}" <<'PY'
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
      echo "    Updating reservation ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME} to baseline=${BQ_BASELINE_SLOTS}, autoscale_max_slots=${desired_autoscale}..."
      bq update --reservation \
        --project_id="${BQ_ADMIN_PROJECT_ID}" \
        --location="${BQ_LOCATION}" \
        --slots="${BQ_BASELINE_SLOTS}" \
        --autoscale_max_slots="${desired_autoscale}" \
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
    if [[ "${BQ_BASELINE_SLOTS}" -lt "${BQ_SLOT_CAP}" ]]; then
      mk_res_cmd+=(--autoscale_max_slots="${BQ_SLOT_CAP}")
    fi
    mk_res_cmd+=("${BQ_RESERVATION_NAME}")
    if ! "${mk_res_cmd[@]}"; then
      echo "ERROR: Failed to create reservation '${BQ_RESERVATION_NAME}'. If this sandbox lacks reservation admin quota, re-run with --skip-reservation." >&2
      exit 1
    fi
    echo "    Created Enterprise reservation ${BQ_RESERVATION_NAME}."
  fi

  assignments_json="$(bq ls --reservation_assignment --project_id="${BQ_ADMIN_PROJECT_ID}" --location="${BQ_LOCATION}" --format=json 2>/dev/null || true)"
  assignment_state="$(python3 - "${assignments_json}" "${PROJECT_ID}" "${PROJECT_NUMBER}" "${BQ_RESERVATION_NAME}" <<'PY'
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
        # Formatted name is either "proj:loc.res.id" or "projects/.../reservations/<res>/assignments/<id>"
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
)"

  case "${assignment_state}" in
    MATCH:*)
      echo "    QUERY assignment already points to ${BQ_RESERVATION_NAME} (${assignment_state#MATCH:})."
      ;;
    CONFLICT:*)
      echo "ERROR: Project '${PROJECT_ID}' already has a QUERY assignment to a different reservation: ${assignment_state#CONFLICT:}" >&2
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
# (f) VPC subnet Private Google Access, internal firewall, Dataproc IAM & clusters
# ---------------------------------------------------------------------------
echo "==> [6/6] Configuring VPC networking, Dataproc IAM, and x86 Dataproc clusters..."
echo "    Granting roles/storage.objectAdmin on gs://${BENCH_BUCKET} to ${DATAPROC_SA}..."
gcloud storage buckets add-iam-policy-binding "gs://${BENCH_BUCKET}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${DATAPROC_SA}" \
  --role="roles/storage.objectAdmin" >/dev/null

subnet_json="$(gcloud compute networks subnets describe default --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" --format=json)"
read -r PRIVATE_PGA SUBNET_CIDR NETWORK_NAME <<<"$(python3 - "${subnet_json}" <<'PY'
import json
import sys

raw = sys.argv[1]
idx = raw.find("{")
obj = json.loads(raw[idx:])
pga = "true" if obj.get("privateIpGoogleAccess") else "false"
cidr = obj.get("ipCidrRange", "")
network_url = obj.get("network", "")
network_name = network_url.rstrip("/").split("/")[-1] if network_url else "default"
print(f"{pga} {cidr} {network_name}")
PY
)"

if [[ "${PRIVATE_PGA}" != "true" ]]; then
  echo "    Enabling privateIpGoogleAccess on subnet 'default' (${DATAPROC_REGION})..."
  gcloud compute networks subnets update default \
    --region="${DATAPROC_REGION}" \
    --project="${PROJECT_ID}" \
    --enable-private-ip-google-access
else
  echo "    Subnet 'default' (${DATAPROC_REGION}) already has privateIpGoogleAccess enabled."
fi

fw_json="$(gcloud compute firewall-rules list --project="${PROJECT_ID}" --format=json)"
HAS_INTERNAL_FW="$(python3 - "${fw_json}" "${NETWORK_NAME}" <<'PY'
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
    if name in ("default-allow-internal", "bench-allow-internal"):
        print("YES")
        sys.exit(0)
print("NO")
PY
)"

if [[ "${HAS_INTERNAL_FW}" != "YES" ]]; then
  if [[ -z "${SUBNET_CIDR}" ]]; then
    echo "ERROR: Unable to determine subnet CIDR for firewall creation." >&2
    exit 1
  fi
  echo "    Creating internal Dataproc communication firewall rule 'bench-allow-internal' (${SUBNET_CIDR})..."
  gcloud compute firewall-rules create "bench-allow-internal" \
    --project="${PROJECT_ID}" \
    --network="${NETWORK_NAME}" \
    --direction=INGRESS \
    --action=ALLOW \
    --rules="tcp:0-65535,udp:0-65535,icmp" \
    --source-ranges="${SUBNET_CIDR}"
else
  echo "    Internal VPC firewall rule already present on network '${NETWORK_NAME}'."
fi

create_dataproc_cluster() {
  local cluster_name="$1"
  local engine_mode="$2"

  if gcloud dataproc clusters describe "${cluster_name}" --region="${DATAPROC_REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
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
    --no-address
    --shielded-secure-boot
    --bucket="${BENCH_BUCKET}"
  )
  if [[ -n "${DATAPROC_SERVICE_ACCOUNT:-}" ]]; then
    cmd+=(--service-account="${DATAPROC_SERVICE_ACCOUNT}")
  fi

  echo "    Creating Dataproc cluster '${cluster_name}' (--engine=${engine_mode}, ${NUM_PRIMARY_WORKERS}x ${WORKER_MACHINE_TYPE}, ${NUM_WORKER_LOCAL_SSDS}x NVMe Local SSD)..."
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
  echo "    Reservation: ${BQ_ADMIN_PROJECT_ID}:${BQ_LOCATION}.${BQ_RESERVATION_NAME} (baseline=${BQ_BASELINE_SLOTS}, max_slots=${BQ_SLOT_CAP})"
fi
if [[ "${CLUSTERS_MODE}" != "none" ]]; then
  echo "    Clusters:    mode=${CLUSTERS_MODE} (${CLUSTER_STD_NAME}, ${CLUSTER_LGT_NAME} in ${DATAPROC_REGION})"
fi
