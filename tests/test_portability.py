"""Tests for regional portability, enterprise org hardening, and identifier leak guard."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import metrics_helper as mh
from scripts import stage_tpcds_cross_region as cross_region

# ---------------------------------------------------------------------------
# 1. Parameterized Regional Pricing (scripts/metrics_helper.py)
# ---------------------------------------------------------------------------


def test_us_list_rates_parity_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "PRICING_REGION",
        "PRICING_GCE_VCPU_HR",
        "PRICING_GCE_RAM_GB_HR",
        "PRICING_DATAPROC_FEE_VCPU_HR",
        "PRICING_LIGHTNING_SURCHARGE_VCPU_HR",
        "PRICING_LOCAL_SSD_USD_GB_MONTH",
        "PRICING_BQ_SLOT_HR_USD",
        "PRICING_GCS_STANDARD_GB_MONTH",
        "PRICING_BQ_ACTIVE_PHYSICAL_GIB_MONTH",
        "PRICING_BQ_LONG_TERM_PHYSICAL_GIB_MONTH",
    ):
        monkeypatch.delenv(var, raising=False)

    for key, expected in mh.US_LIST_RATES.items():
        assert mh.get_list_rates(None)[key] == expected
        assert mh.get_list_rates("US")[key] == expected
        assert mh.get_list_rates("us-central1")[key] == expected

    us_rates = mh.get_list_rates("US")
    assert us_rates["storage_monthly_usd"] == {
        "gcs_standard_per_gb": 0.026,
        "bq_active_physical_per_gib": 0.040,
        "bq_long_term_physical_per_gib": 0.020,
    }

    # Verify exact US numeric equivalence against baseline pricing calculations
    spark_cost = mh.estimate_stage_list_cost_usd(
        engine_variant="spark_lightning_nqe",
        wall_clock_sec=360.0,
        worker_machine_type="n2d-standard-8",
        total_worker_vcpus=16,
    )
    assert spark_cost is not None and spark_cost > 0
    assert spark_cost == mh.estimate_stage_list_cost_usd(
        engine_variant="spark_lightning_nqe",
        wall_clock_sec=360.0,
        worker_machine_type="n2d-standard-8",
        total_worker_vcpus=16,
        region_or_location="US",
    )

    bq_cost = mh.estimate_stage_list_cost_usd(
        engine_variant="bigframes_native_bq",
        wall_clock_sec=100.0,
        bq_total_slot_ms=3_600_000,
    )
    assert bq_cost == 0.06


def test_regional_rates_asia_southeast1_and_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    sg_rates = mh.get_list_rates("ASIA-SOUTHEAST1")
    assert sg_rates["bq_enterprise_slot_hr_usd"] == 0.072
    assert sg_rates["storage_monthly_usd"]["gcs_standard_per_gb"] == 0.020
    assert sg_rates["storage_monthly_usd"]["bq_active_physical_per_gib"] == 0.044

    sg_bq_cost = mh.estimate_stage_list_cost_usd(
        engine_variant="bigframes_ctas_native_bq",
        wall_clock_sec=60.0,
        bq_total_slot_ms=3_600_000,
        region_or_location="asia-southeast1",
    )
    assert sg_bq_cost == 0.072

    monkeypatch.setenv("PRICING_REGION", "asia-southeast1")
    monkeypatch.setenv("PRICING_BQ_SLOT_HR_USD", "0.090")
    overridden = mh.get_list_rates()
    assert overridden["bq_enterprise_slot_hr_usd"] == 0.090
    assert mh.US_LIST_RATES["bq_enterprise_slot_hr_usd"] == 0.060


def test_resolve_bench_config_biglake_suffix_by_location(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BIGLAKE_CONNECTION_ID", raising=False)
    monkeypatch.delenv("BQ_LOCATION", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text('PROJECT_ID="demo-proj"\nBQ_LOCATION="asia-southeast1"\n', encoding="utf-8")

    cfg = mh.resolve_bench_config(env_path=env_file, repo_root=tmp_path, populate_environ=False)
    assert cfg["BIGLAKE_CONNECTION_ID"] == "bench-biglake-asia-southeast1"


# ---------------------------------------------------------------------------
# 2. Cross-Region Storage-Read Staging (scripts/stage_tpcds_cross_region.py)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bq_loc", "scale"),
    [
        ("US", "small_1g"),
        ("US", "medium_100g"),
        ("US", "large_500g"),
        ("asia-southeast1", "small_1g"),
        ("us-central1", "small_1g"),
    ],
)
def test_validate_cross_region_allowed_combinations(bq_loc: str, scale: str) -> None:
    cross_region.validate_cross_region_request(bq_loc, scale)


@pytest.mark.parametrize(
    ("bq_loc", "scale"),
    [
        ("asia-southeast1", "medium_100g"),
        ("asia-southeast1", "large_500g"),
        ("us-central1", "medium_100g"),
        ("europe-west4", "large_500g"),
    ],
)
def test_validate_cross_region_fails_fast_non_us_large_scales(bq_loc: str, scale: str) -> None:
    with pytest.raises(ValueError, match="only supports SCALE_PRESET='small_1g'"):
        cross_region.validate_cross_region_request(bq_loc, scale)

    rc = cross_region.main(["--bq-location", bq_loc, "--scale-preset", scale])
    assert rc == 1


def test_stage_cli_dry_run_derives_regional_connection(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ENV_FILE", "/dev/null")
    for var in ("PROJECT_ID", "PROJECT_NUMBER", "BENCH_BUCKET", "BIGLAKE_CONNECTION_ID", "BQ_LOCATION", "GCS_LOCATION"):
        monkeypatch.delenv(var, raising=False)
    rc = cross_region.main(
        ["--bq-location", "asia-southeast1", "--scale-preset", "small_1g", "--dry-run"]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "bq_location=asia-southeast1" in out
    assert "conn=bench-biglake-asia-southeast1" in out
    assert "gcs_prefix=gs://<BENCH_BUCKET>/bronze/small_1g/<table>/" in out


def test_stage_table_cross_region_streams_arrow_to_snappy_parquet_and_regional_bq(
    tmp_path: Path,
) -> None:
    # Build real Arrow IPC schema + record batch
    schema = pa.schema(
        [
            pa.field("ss_sold_date_sk", pa.int64()),
            pa.field("ss_item_sk", pa.int64()),
            pa.field("ss_net_paid", pa.float64()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([2452276, 2452277], type=pa.int64()),
            pa.array([101, 202], type=pa.int64()),
            pa.array([12.5, 45.0], type=pa.float64()),
        ],
        schema=schema,
    )
    serialized_schema = schema.serialize().to_pybytes()
    serialized_batch = batch.serialize().to_pybytes()

    mock_session = SimpleNamespace(
        arrow_schema=SimpleNamespace(serialized_schema=serialized_schema),
        streams=[SimpleNamespace(name="projects/p/locations/us/sessions/s/streams/0")],
    )
    mock_response = SimpleNamespace(
        arrow_record_batch=SimpleNamespace(serialized_record_batch=serialized_batch)
    )

    read_client = MagicMock()
    read_client.create_read_session.return_value = mock_session
    read_client.read_rows.return_value = [mock_response]

    uploaded_files: dict[str, bytes] = {}
    gcs_client = MagicMock()
    gcs_client.list_blobs.return_value = []

    def _make_blob(blob_name: str) -> MagicMock:
        blob = MagicMock()

        def _upload(filename: str) -> None:
            uploaded_files[blob_name] = Path(filename).read_bytes()

        blob.upload_from_filename.side_effect = _upload
        return blob

    bucket_mock = MagicMock()
    bucket_mock.blob.side_effect = _make_blob
    gcs_client.bucket.return_value = bucket_mock

    bq_client = MagicMock()
    bq_client.load_table_from_uri.return_value.result.return_value = None
    bq_client.query.return_value.result.return_value = None

    stats = cross_region.stage_table_cross_region(
        table_name="store_sales",
        project_id="example-project",
        bq_location="asia-southeast1",
        bench_bucket="lakehouse-bench-example",
        ds_bronze_native="bench_bronze_native",
        ds_bronze_biglake="bench_bronze_biglake",
        biglake_connection_id="bench-biglake-asia-southeast1",
        gcs_prefix_base="bronze/small_1g",
        ingest_mode="both",
        bq_client=bq_client,
        gcs_client=gcs_client,
        read_client=read_client,
    )

    assert stats["streamed_rows"] == 2
    assert stats["parquet_shards"] == 1
    assert "bronze/small_1g/store_sales/part-00000.snappy.parquet" in uploaded_files

    # Read uploaded Parquet bytes back to verify valid Snappy Parquet contents
    parquet_file = tmp_path / "verify.parquet"
    parquet_file.write_bytes(uploaded_files["bronze/small_1g/store_sales/part-00000.snappy.parquet"])
    restored = pq.read_table(parquet_file)
    assert restored.num_rows == 2
    assert restored.column("ss_net_paid").to_pylist() == [12.5, 45.0]

    # Verify regional BQ LoadJobConfig clustering & non-US location
    create_kwargs = read_client.create_read_session.call_args.kwargs
    assert create_kwargs["parent"] == "projects/example-project"
    assert (
        create_kwargs["read_session"].table
        == "projects/bigquery-public-data/datasets/tpc_ds_1g/tables/store_sales"
    )

    load_args, load_kwargs = bq_client.load_table_from_uri.call_args
    assert load_args[1] == "example-project.bench_bronze_native.store_sales"
    assert load_kwargs["location"] == "asia-southeast1"
    assert load_kwargs["job_config"].clustering_fields == ["ss_sold_date_sk"]

    query_sql = bq_client.query.call_args.args[0]
    assert "`example-project.asia-southeast1.bench-biglake-asia-southeast1`" in query_sql
    assert bq_client.query.call_args.kwargs["location"] == "asia-southeast1"


# ---------------------------------------------------------------------------
# 3. Shell Scripts: Enterprise Org Hardening & Repo A Interop (Mocked CLI)
# ---------------------------------------------------------------------------


def _write_mock_gcp_bin(tmp_path: Path, *, scenario: dict[str, object]) -> tuple[Path, Path]:
    """Create stubbed gcloud and bq executables recording every CLI call."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    log_file = tmp_path / "gcp_calls.log"
    log_file.write_text("", encoding="utf-8")
    state_file = tmp_path / "mock_state.json"
    state_file.write_text(json.dumps(scenario), encoding="utf-8")

    gcloud_stub = bin_dir / "gcloud"
    gcloud_stub.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

log = Path({str(log_file)!r})
state = json.loads(Path({str(state_file)!r}).read_text())
args = sys.argv[1:]
with log.open("a") as f:
    f.write("gcloud " + " ".join(args) + "\\n")

cmd3 = tuple(args[:3])
cmd4 = tuple(args[:4])

if args[:2] == ["projects", "describe"]:
    print("99900011122" if "--format=value(projectNumber)" in args else "{{}}")
    sys.exit(0)
if args[:2] == ["services", "enable"]:
    sys.exit(0)
if cmd3 == ("storage", "buckets", "describe"):
    target = args[3].removeprefix("gs://")
    sys.exit(0 if target in state.get("buckets", []) else 1)
if cmd3 == ("storage", "buckets", "list"):
    want_json = any(a.startswith("--format=json") for a in args)
    locs = state.get("bucket_locations", {{}})
    if want_json:
        out = []
        for b in state.get("buckets", []):
            loc = locs.get(b, "")
            if not loc:
                if "-asia-southeast1-" in b:
                    loc = "ASIA-SOUTHEAST1"
                elif "-us-central1-" in b:
                    loc = "US-CENTRAL1"
                else:
                    loc = state.get("default_bucket_location", "US")
            out.append({{"name": b, "location": loc}})
        print(json.dumps(out))
    else:
        for b in state.get("buckets", []):
            print(b)
    sys.exit(0)
if cmd3 == ("storage", "buckets", "create"):
    b = args[3].removeprefix("gs://")
    state.setdefault("buckets", []).append(b)
    Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if cmd3 in (("storage", "buckets", "update"), ("storage", "buckets", "add-iam-policy-binding"), ("storage", "rm")):
    sys.exit(0)
if cmd3 == ("iam", "service-accounts", "describe"):
    target = args[3]
    sys.exit(0 if target in state.get("sas", []) else 1)
if cmd3 == ("iam", "service-accounts", "create"):
    sa_email = args[3] + "@" + state["project_id"] + ".iam.gserviceaccount.com"
    state.setdefault("sas", []).append(sa_email)
    Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if cmd3 == ("iam", "service-accounts", "delete"):
    sys.exit(0)
if args[:2] == ["projects", "add-iam-policy-binding"] or args[:2] == ["projects", "remove-iam-policy-binding"]:
    sys.exit(0)
if cmd4 == ("compute", "networks", "subnets", "describe"):
    sub = args[4]
    subs = state.get("subnets", {{}})
    if sub not in subs:
        sys.exit(1)
    print(json.dumps(subs[sub]))
    sys.exit(0)
if cmd4 == ("compute", "networks", "subnets", "list"):
    print(json.dumps(list(state.get("subnets", {{}}).values())))
    sys.exit(0)
if cmd4 == ("compute", "networks", "subnets", "create"):
    sub = args[4]
    net = next((a.split("=", 1)[1] for a in args if a.startswith("--network=")), "bench-vpc")
    cidr = next((a.split("=", 1)[1] for a in args if a.startswith("--range=")), "10.20.0.0/24")
    state.setdefault("subnets", {{}})[sub] = {{
        "name": sub,
        "ipCidrRange": cidr,
        "privateIpGoogleAccess": "--enable-private-ip-google-access" in args,
        "network": f"https://compute/projects/p/global/networks/{{net}}",
    }}
    state.setdefault("networks", {{}}).setdefault(net, {{"name": net, "subnetworks": []}})["subnetworks"].append(sub)
    Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if cmd4 == ("compute", "networks", "subnets", "update") or cmd4 == ("compute", "networks", "subnets", "delete"):
    sys.exit(0)
if cmd3 == ("compute", "networks", "describe"):
    net = args[3]
    nets = state.get("networks", {{}})
    if net not in nets:
        sys.exit(1)
    print(json.dumps(nets[net]))
    sys.exit(0)
if cmd3 == ("compute", "networks", "create"):
    net = args[3]
    state.setdefault("networks", {{}})[net] = {{"name": net, "subnetworks": []}}
    Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if cmd3 == ("compute", "networks", "delete"):
    sys.exit(0)
if cmd3 == ("compute", "firewall-rules", "list"):
    print(json.dumps(state.get("firewalls", [])))
    sys.exit(0)
if cmd3 in (("compute", "firewall-rules", "create"), ("compute", "firewall-rules", "delete")):
    sys.exit(0)
if cmd3 == ("compute", "regions", "describe"):
    quotas = state.get("quotas", [
        {{"metric": "N2D_CPUS", "limit": 64.0, "usage": 0.0}},
        {{"metric": "CPUS", "limit": 64.0, "usage": 0.0}},
    ])
    print(json.dumps({{"quotas": quotas}}))
    sys.exit(0)
if cmd3 == ("dataproc", "clusters", "describe"):
    c = args[3]
    sys.exit(0 if c in state.get("clusters", []) else 1)
if cmd3 == ("dataproc", "clusters", "create") or cmd3 == ("dataproc", "clusters", "delete"):
    sys.exit(0)
sys.exit(0)
""",
        encoding="utf-8",
    )
    gcloud_stub.chmod(gcloud_stub.stat().st_mode | stat.S_IEXEC)

    bq_stub = bin_dir / "bq"
    bq_stub.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

log = Path({str(log_file)!r})
state = json.loads(Path({str(state_file)!r}).read_text())
args = sys.argv[1:]
with log.open("a") as f:
    f.write("bq " + " ".join(args) + "\\n")

subcmd = args[0] if args else ""
if subcmd == "show":
    if "--connection" in args:
        conn = args[-1]
        if conn in state.get("connections", []):
            print(json.dumps({{"cloudResource": {{"serviceAccountId": "bqcx-test@gcp-sa-bigqueryconnection.iam.gserviceaccount.com"}}}}))
            sys.exit(0)
        sys.exit(1)
    if "--reservation" in args:
        res = args[-1]
        if res in state.get("reservations", {{}}):
            print(json.dumps(state["reservations"][res]))
            sys.exit(0)
        sys.exit(1)
    ds = args[-1].split(":")[-1]
    if "--format=json" in args and ds in state.get("datasets", []):
        print(json.dumps({{"access": state.get("dataset_access", {{}}).get(ds, [])}}))
        sys.exit(0)
    sys.exit(0 if ds in state.get("datasets", []) else 1)
if subcmd == "mk":
    if "--connection" in args:
        state.setdefault("connections", []).append(args[-1])
        Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if subcmd == "ls" and "--reservation_assignment" in args:
    print(json.dumps(state.get("assignments", [])))
    sys.exit(0)
if subcmd == "query":
    if "--format=csv" in args:
        print("option_value\\nPHYSICAL")
    sys.exit(0)
if subcmd == "update":
    for a in args:
        if a.startswith("--source="):
            src = Path(a.split("=", 1)[1])
            state["last_bq_update"] = json.loads(src.read_text())
            Path({str(state_file)!r}).write_text(json.dumps(state))
    sys.exit(0)
if subcmd in ("add-iam-policy-binding", "rm"):
    sys.exit(0)
sys.exit(0)
""",
        encoding="utf-8",
    )
    bq_stub.chmod(bq_stub.stat().st_mode | stat.S_IEXEC)
    return bin_dir, log_file


def test_provision_reuses_repo_a_subnet_and_reservation_safely(tmp_path: Path) -> None:
    bin_dir, log_file = _write_mock_gcp_bin(
        tmp_path,
        scenario={
            "project_id": "example-proj",
            "buckets": ["lakehouse-bench-99900011122"],
            "connections": ["bench-biglake-asia-southeast1"],
            "datasets": [
                "bench_bronze_biglake",
                "bench_bronze_native",
                "bench_silver_bq",
                "bench_gold_bq",
                "bench_metrics",
            ],
            "reservations": {
                "sandbox-elt-cap": {
                    "edition": "ENTERPRISE",
                    "slotCapacity": 0,
                    "autoscale": {"maxSlots": 100},
                }
            },
            "assignments": [
                {
                    "jobType": "QUERY",
                    "assignee": "projects/example-proj",
                    "name": "example-proj:asia-southeast1.sandbox-elt-cap.assign1",
                }
            ],
            "subnets": {
                "sandbox-pga-asia-southeast1": {
                    "name": "sandbox-pga-asia-southeast1",
                    "ipCidrRange": "10.10.0.0/24",
                    "privateIpGoogleAccess": True,
                    "network": "https://compute/projects/p/global/networks/sandbox-vpc",
                }
            },
            "firewalls": [
                {
                    "name": "sandbox-allow-internal",
                    "disabled": False,
                    "direction": "INGRESS",
                    "network": "https://compute/projects/p/global/networks/sandbox-vpc",
                }
            ],
            "quotas": [
                {"metric": "N2D_CPUS", "limit": 32.0, "usage": 0.0},
                {"metric": "CPUS", "limit": 32.0, "usage": 0.0},
            ],
        },
    )
    env = dict(os.environ)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "asia-southeast1",
            "GCS_LOCATION": "asia-southeast1",
            "DATAPROC_REGION": "asia-southeast1",
            "DATAPROC_ZONE": "asia-southeast1-a",
        }
    )

    proc = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "provision_infra.sh"),
            "--clusters=std",
            "--subnet=sandbox-pga-asia-southeast1",
            "--reuse-reservation=sandbox-elt-cap",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")

    # Never resizes/patches Repo A's reservation or touches 'default' network
    assert "bq mk --reservation" not in calls
    assert "bq update --reservation" not in calls
    assert "subnets update default" not in calls
    assert "subnets describe default" not in calls
    assert "firewall-rules create" not in calls  # sandbox-allow-internal already matches

    # Dedicated least-privilege SA + --condition=None + cluster flags
    assert "iam service-accounts create bench-dataproc-runner" in calls
    assert "--condition=None" in calls
    assert "--service-account=bench-dataproc-runner@example-proj.iam.gserviceaccount.com" in calls
    assert "--subnet=sandbox-pga-asia-southeast1" in calls
    assert "--max-idle=30m" in calls
    assert "--max-age=4h" in calls
    assert "bench-spark-std-x86" in calls
    assert "bench-spark-lightning-x86" not in calls
    assert "Verified reservation example-proj:asia-southeast1.sandbox-elt-cap" in proc.stdout


def test_provision_auto_creates_isolated_bench_vpc_and_fails_on_low_quota(tmp_path: Path) -> None:
    bin_dir, log_file = _write_mock_gcp_bin(
        tmp_path,
        scenario={
            "project_id": "example-proj",
            "buckets": ["lakehouse-bench-99900011122"],
            "connections": ["bench-biglake-asia-southeast1"],
            "datasets": [
                "bench_bronze_biglake",
                "bench_bronze_native",
                "bench_silver_bq",
                "bench_gold_bq",
                "bench_metrics",
            ],
            "quotas": [
                {"metric": "N2D_CPUS", "limit": 24.0, "usage": 0.0},
                {"metric": "CPUS", "limit": 32.0, "usage": 0.0},
            ],
        },
    )
    env = dict(os.environ)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "asia-southeast1",
            "GCS_LOCATION": "asia-southeast1",
            "DATAPROC_REGION": "asia-southeast1",
            "DATAPROC_ZONE": "asia-southeast1-a",
        }
    )

    proc = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "provision_infra.sh"),
            "--clusters=both",
            "--skip-reservation",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    calls = log_file.read_text(encoding="utf-8")

    # Auto-created isolated bench-vpc + bench-pga-* + bench-allow-internal without touching default
    assert "compute networks create bench-vpc --project=example-proj --subnet-mode=custom" in calls
    assert "compute networks subnets create bench-pga-asia-southeast1" in calls
    assert "compute firewall-rules create bench-allow-internal" in calls
    assert "default" not in [tok for line in calls.splitlines() for tok in line.split() if tok == "default"]

    # Quota preflight blocked half-creating 2 clusters when N2D_CPUS=24 < 40
    assert proc.returncode == 1
    assert "--clusters=std" in proc.stderr
    assert "dataproc clusters create" not in calls


def test_provision_rejects_default_compute_sa(tmp_path: Path) -> None:
    bin_dir, _ = _write_mock_gcp_bin(tmp_path, scenario={"project_id": "example-proj"})
    env = dict(os.environ)
    default_sa = "99900011122" + "-comp" + "ute@devel" + "oper.gserviceaccount.com"
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "DATAPROC_SERVICE_ACCOUNT": default_sa,
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "provision_infra.sh"), "--clusters=none"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "disallowed" in proc.stderr.lower()


def test_provision_defaults_gcs_to_bq_location_and_rejects_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BQ_LOCATION", "us-central1")
    monkeypatch.delenv("GCS_LOCATION", raising=False)
    resolved = mh.resolve_bench_config(
        env_path=tmp_path / "missing.env",
        repo_root=REPO_ROOT,
        populate_environ=False,
    )
    assert resolved["GCS_LOCATION"] == "us-central1"
    assert resolved["BIGLAKE_CONNECTION_ID"] == "bench-biglake-us-central1"

    bin_dir, log_file = _write_mock_gcp_bin(tmp_path, scenario={"project_id": "example-proj"})
    env = dict(os.environ)
    env.pop("GCS_LOCATION", None)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "us-central1",
            "DATAPROC_REGION": "us-central1",
            "DATAPROC_ZONE": "us-central1-a",
        }
    )
    subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "provision_infra.sh"),
            "--clusters=none",
            "--skip-reservation",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")
    assert (
        "storage buckets create gs://lakehouse-bench-99900011122 --project=example-proj --location=us-central1"
        in calls
    )
    assert "--location=US" not in calls

    # Explicit non-US BQ_LOCATION vs mismatched GCS_LOCATION=US must fail fast before GCP calls
    mismatch_tmp = tmp_path / "mismatch"
    mismatch_bin, mismatch_log = _write_mock_gcp_bin(mismatch_tmp, scenario={"project_id": "example-proj"})
    bad_env = dict(env)
    bad_env.update(
        {
            "PATH": f"{mismatch_bin}:{os.environ['PATH']}",
            "BQ_LOCATION": "asia-southeast1",
            "GCS_LOCATION": "US",
        }
    )
    bad_proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "provision_infra.sh"), "--clusters=none"],
        cwd=mismatch_tmp,
        env=bad_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad_proc.returncode != 0
    assert "co-located GCS_LOCATION" in bad_proc.stderr
    assert mismatch_log.read_text(encoding="utf-8") == ""


def test_teardown_granular_clusters_protects_repo_a_and_purges_bench_and_dataproc_buckets(
    tmp_path: Path,
) -> None:
    bin_dir, log_file = _write_mock_gcp_bin(
        tmp_path,
        scenario={
            "project_id": "example-proj",
            "clusters": ["bench-spark-std-x86", "bench-spark-lightning-x86"],
            "buckets": [
                "lakehouse-bench-99900011122",
                "dataproc-temp-asia-southeast1-1234",
                "dataproc-staging-asia-southeast1-1234",
                "dataproc-temp-us-central1-9999",
                "airport-ops-raw-999",
            ],
            "sas": ["bench-dataproc-runner@example-proj.iam.gserviceaccount.com"],
            "firewalls": [
                {
                    "name": "sandbox-allow-internal",
                    "network": "https://compute/projects/p/global/networks/sandbox-vpc",
                },
                {
                    "name": "bench-allow-internal",
                    "network": "https://compute/projects/p/global/networks/bench-vpc",
                },
            ],
            "subnets": {
                "sandbox-pga-asia-southeast1": {
                    "name": "sandbox-pga-asia-southeast1",
                    "network": "https://compute/projects/p/global/networks/sandbox-vpc",
                },
                "bench-pga-asia-southeast1": {
                    "name": "bench-pga-asia-southeast1",
                    "network": "https://compute/projects/p/global/networks/bench-vpc",
                },
            },
            "networks": {
                "bench-vpc": {"name": "bench-vpc", "subnetworks": []},
                "sandbox-vpc": {"name": "sandbox-vpc", "subnetworks": ["sandbox-pga-asia-southeast1"]},
            },
            "reservations": {
                "sandbox-elt-cap": {"slotCapacity": 0, "autoscale": {"maxSlots": 100}},
            },
        },
    )
    env = dict(os.environ)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "asia-southeast1",
            "DATAPROC_REGION": "asia-southeast1",
            "BQ_RESERVATION_NAME": "sandbox-elt-cap",
        }
    )

    subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "teardown_infra.sh"),
            "--confirm",
            "--clusters=std",
            "--keep-reservation",
            "--purge-data",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")

    # Granular cluster deletion
    assert "dataproc clusters delete bench-spark-std-x86" in calls
    assert "bench-spark-lightning-x86" not in calls

    # Preserved reservation
    assert "bq rm --reservation" not in calls

    # Soft-delete patch + deletion on bench & matching-region dataproc-temp/staging;
    # other-region dataproc-temp-* and airport-ops-* remain untouched
    assert "storage rm --recursive --quiet gs://lakehouse-bench-99900011122" in calls
    assert "storage rm --recursive --quiet gs://dataproc-temp-asia-southeast1-1234" in calls
    assert "storage rm --recursive --quiet gs://dataproc-staging-asia-southeast1-1234" in calls
    assert "dataproc-temp-us-central1-9999" not in calls
    assert "airport-ops-raw-999" not in calls

    # Bench SA & VPC deleted; Repo A sandbox-* resources completely untouched
    assert "iam service-accounts delete bench-dataproc-runner@example-proj.iam.gserviceaccount.com" in calls
    assert "firewall-rules delete bench-allow-internal" in calls
    assert "subnets delete bench-pga-asia-southeast1" in calls
    assert "networks delete bench-vpc" in calls
    assert "sandbox-allow-internal" not in calls
    assert "sandbox-pga-asia-southeast1" not in calls
    assert "sandbox-vpc" not in calls


def test_teardown_protects_sandbox_elt_cap_even_without_keep_flag(tmp_path: Path) -> None:
    bin_dir, log_file = _write_mock_gcp_bin(
        tmp_path,
        scenario={
            "project_id": "example-proj",
            "reservations": {
                "sandbox-elt-cap": {"slotCapacity": 0, "autoscale": {"maxSlots": 100}},
            },
        },
    )
    env = dict(os.environ)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_RESERVATION_NAME": "sandbox-elt-cap",
        }
    )
    subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "teardown_infra.sh"), "--confirm", "--clusters=none"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")
    assert "bq rm --reservation" not in calls
    assert "bq update --reservation" not in calls


# ---------------------------------------------------------------------------
# 4. Identifier Leak Guard (scripts/check_identifiers.sh)
# ---------------------------------------------------------------------------


def test_check_identifiers_catches_leaks_and_passes_clean_files(tmp_path: Path) -> None:
    clean_file = tmp_path / "clean.py"
    clean_file.write_text('PROJECT = "example-project"\n', encoding="utf-8")
    clean_proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "check_identifiers.sh"), str(clean_file)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert clean_proc.returncode == 0

    # Construct synthetic test leaks dynamically so test_portability.py stays leak-free
    leak_file = tmp_path / "leaky.txt"
    fake_email = "alice@" + "goo" + "gle.com"
    fake_proj = "demo-" + "play" + "ground-123456"
    fake_num = str(111111 * 1000000 + 222222)
    leak_file.write_text(
        f"email={fake_email}\nproj={fake_proj}\nnum={fake_num}\n",
        encoding="utf-8",
    )
    leak_proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "check_identifiers.sh"), str(leak_file)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert leak_proc.returncode == 1
    assert "corp_email" in leak_proc.stderr
    assert "playground_project" in leak_proc.stderr
    assert "twelve_digit_number" in leak_proc.stderr

    env_extra = dict(os.environ)
    env_extra["EXTRA_FORBIDDEN_REGEX"] = r"FORBIDDEN_CUSTOM_TOKEN"
    custom_file = tmp_path / "custom.txt"
    custom_file.write_text("value = 'FORBIDDEN_CUSTOM_TOKEN'\n", encoding="utf-8")
    custom_proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "check_identifiers.sh"), str(custom_file)],
        env=env_extra,
        capture_output=True,
        text=True,
        check=False,
    )
    assert custom_proc.returncode == 1
    assert "extra_forbidden_regex" in custom_proc.stderr


def test_dotenv_does_not_overwrite_caller_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    custom_env = tmp_path / "custom.env"
    custom_env.write_text(
        'export PROJECT_ID="dotenv-proj"\n'
        'export PROJECT_NUMBER="11122233344"\n'
        'export BQ_LOCATION="US"\n'
        'export GCS_LOCATION="${BQ_LOCATION}"\n'
        'export DATAPROC_REGION="europe-west4"\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("PROJECT_ID", "example-proj")
    monkeypatch.setenv("PROJECT_NUMBER", "99900011122")
    monkeypatch.setenv("BQ_LOCATION", "us-central1")
    monkeypatch.delenv("GCS_LOCATION", raising=False)
    monkeypatch.delenv("DATAPROC_REGION", raising=False)

    cfg = mh.resolve_bench_config(env_path=custom_env, repo_root=REPO_ROOT, populate_environ=False)
    assert cfg["PROJECT_ID"] == "example-proj"
    assert cfg["PROJECT_NUMBER"] == "99900011122"
    assert cfg["BQ_LOCATION"] == "us-central1"
    assert cfg["GCS_LOCATION"] == "us-central1"
    assert cfg["DATAPROC_REGION"] == "europe-west4"

    bin_dir, log_file = _write_mock_gcp_bin(tmp_path, scenario={"project_id": "example-proj"})
    env = dict(os.environ)
    env.pop("GCS_LOCATION", None)
    env.update(
        {
            "ENV_FILE": str(custom_env),
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "us-central1",
            "DATAPROC_REGION": "us-central1",
            "DATAPROC_ZONE": "us-central1-a",
        }
    )
    subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "provision_infra.sh"),
            "--clusters=none",
            "--skip-reservation",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")
    assert (
        "storage buckets create gs://lakehouse-bench-99900011122 --project=example-proj --location=us-central1"
        in calls
    )
    assert "dotenv-proj" not in calls
    assert "--location=US" not in calls


def test_provision_grants_metrics_dataset_acl_via_bq_update(tmp_path: Path) -> None:
    bin_dir, log_file = _write_mock_gcp_bin(
        tmp_path,
        scenario={
            "project_id": "example-proj",
            "datasets": [
                "bench_bronze_biglake",
                "bench_bronze_native",
                "bench_silver_bq",
                "bench_gold_bq",
                "bench_metrics",
            ],
            "dataset_access": {
                "bench_metrics": [{"role": "OWNER", "specialGroup": "projectOwners"}]
            },
        },
    )
    env = dict(os.environ)
    env.update(
        {
            "ENV_FILE": "/dev/null",
            "PATH": f"{bin_dir}:{env['PATH']}",
            "PROJECT_ID": "example-proj",
            "PROJECT_NUMBER": "99900011122",
            "BQ_LOCATION": "us-central1",
            "GCS_LOCATION": "us-central1",
            "DATAPROC_REGION": "us-central1",
            "DATAPROC_ZONE": "us-central1-a",
        }
    )
    subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "scripts" / "provision_infra.sh"),
            "--clusters=none",
            "--skip-reservation",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    calls = log_file.read_text(encoding="utf-8")
    assert "bq add-iam-policy-binding" not in calls
    assert "bq update --project_id=example-proj --source=" in calls
    state = json.loads((tmp_path / "mock_state.json").read_text(encoding="utf-8"))
    assert state["last_bq_update"]["access"] == [
        {"role": "OWNER", "specialGroup": "projectOwners"},
        {
            "role": "WRITER",
            "userByEmail": "bench-dataproc-runner@example-proj.iam.gserviceaccount.com",
        },
    ]
