# Air-Gapped Spark Python Dependency Packaging (`--no-address` / No Cloud NAT)

Dataproc on Compute Engine clusters created by `scripts/provision_infra.sh` launch with `--no-address` on a Private Google Access subnet and **no Cloud NAT** (`constraints/compute.vmExternalIpAccess` safe). Driver and executor VMs can reach Google APIs (`*.googleapis.com`, GCS, BigQuery, Artifact Registry) over Private Google Access, **not** public PyPI.

When Spark jobs or PySpark UDFs need third-party Python packages beyond the base Dataproc image (`DATAPROC_IMAGE_VERSION`, default `2.3-debian12`), use one of the three supported enterprise packaging modes below.

---

## Mode A — GCE Custom Golden Image (Baked Dependencies)

Pre-bake OS libraries, Python wheels, and CA/config artifacts into an immutable GCE VM image before cluster creation using Dataproc's custom image builder (`generate_custom_image.py`).

**When to use:** Standardized platform images reused across many clusters/teams where startup speed matters most and package updates follow a scheduled release cadence.

**How it works:**
1. Author an idempotent `--customization-script` (Debian `apt-get` / `uv pip install` against an internal wheelhouse or temporary build subnet) pinned to `2.3-debian12`.
2. Build the GCE custom image with `--dataproc-version="2.3-debian12"` and store it in your project or Shared Image project.
3. Pass `--image=<custom-image-uri>` (instead of `--image-version`) when creating Dataproc clusters.

**Trade-offs:**
- Zero per-job dependency ship overhead and guaranteed driver/executor parity.
- Requires an image-build pipeline (with temporary mirror/egress access during bake time, separate from runtime `--no-address` VPCs) and periodic rebuilds for CVEs.

---

## Mode C — Relocatable Archive Shipping (`--archives` + `spark.pyspark.python`)

Package a self-contained virtual environment on a Linux x86_64 host matching Debian 12 / the cluster's Python minor version, upload the tarball to GCS, and distribute it to every executor via Spark `--archives`.

**When to use:** Per-pipeline Python dependencies (e.g., `pandas`, `pyarrow`, `scikit-learn`, pure-Python/manylinux wheels) without managing VM or container images.

**Build and stage (`uv` + `venv-pack`):**
```bash
# On a Linux x86_64 build host matching the Dataproc image Python version:
uv venv --python 3.11 .spark-venv
uv pip install --python .spark-venv/bin/python venv-pack pandas pyarrow numpy
.spark-venv/bin/venv-pack -p .spark-venv -o deps.tar.gz

gcloud storage cp deps.tar.gz "gs://${BENCH_BUCKET}/artifacts/deps.tar.gz"
```

**Submit on `--no-address` Dataproc clusters:**
```bash
gcloud dataproc jobs submit pyspark \
  --project="${PROJECT_ID}" \
  --region="${DATAPROC_REGION}" \
  --cluster="${CLUSTER_LGT_NAME}" \
  --archives="gs://${BENCH_BUCKET}/artifacts/deps.tar.gz#venv" \
  --properties="spark.pyspark.python=./venv/bin/python,spark.pyspark.driver.python=./venv/bin/python" \
  scripts/run_spark_medallion.py -- --help
```

> **Note:** On YARN cluster deploy mode (`--properties="spark.submit.deployMode=cluster"`), also set `spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python` so both the ApplicationMaster driver and executors resolve the unpacked `./venv` symlink.

**Trade-offs:**
- No Docker registry or Compute Engine image build required; ships cleanly over GCS Private Google Access.
- Must match OS ABI (`manylinux`/Debian 12) and Python minor version between build host and Dataproc runtime.

---

## Mode D — Custom Container Image in Artifact Registry (`spark` UID/GID `1099`)

Bundle Python packages, native shared libraries, and user code inside a custom OCI container hosted in regional Artifact Registry (`${DATAPROC_REGION}-docker.pkg.dev`).

**When to use:** Dataproc Serverless (`gcloud dataproc batches submit`), Docker-enabled Spark workloads, or heavy C/C++ native dependencies requiring full filesystem control.

**Dockerfile & Artifact Registry permissions:**
```dockerfile
FROM debian:bookworm-slim

# Non-root Spark user expected by Dataproc Serverless / Spark runtime images
RUN groupadd --gid 1099 spark \
 && useradd --uid 1099 --gid 1099 --create-home --shell /bin/bash spark

# Install Python & dependencies into an immutable system venv readable by UID 1099
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/spark-venv \
 && /opt/spark-venv/bin/pip install --no-cache-dir pyarrow pandas

ENV PYSPARK_PYTHON=/opt/spark-venv/bin/python
USER 1099:1099
```

**Push & grant least-privilege pull IAM to the cluster service account:**
```bash
IMAGE_URI="${DATAPROC_REGION}-docker.pkg.dev/${PROJECT_ID}/bench-containers/spark-py:latest"
gcloud artifacts repositories create bench-containers \
  --project="${PROJECT_ID}" \
  --location="${DATAPROC_REGION}" \
  --repository-format=docker

docker push "${IMAGE_URI}"

gcloud artifacts repositories add-iam-policy-binding bench-containers \
  --project="${PROJECT_ID}" \
  --location="${DATAPROC_REGION}" \
  --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
  --role="roles/artifactregistry.reader"
```

Pass `--container-image="${IMAGE_URI}"` on `gcloud dataproc batches submit pyspark` (or configure `spark.executorEnv.PYSPARK_PYTHON=/opt/spark-venv/bin/python` with custom container configuration on Dataproc).

**Trade-offs:**
- Strongest reproducibility and full native-library isolation; pulls cleanly over Private Google Access (`pkg.dev` Private Google Access VIPs).
- Always keep final runtime `USER 1099:1099` (`spark` UID/GID `1099`) so Dataproc Serverless and restricted pod security standards accept the image without root escalation.
