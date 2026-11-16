"""Shared telemetry logger and configuration helper for the Lakehouse Compute Benchmark.

Stdlib-first module imported by notebooks 00-04 and benchmark runners.
Writes structured 21-field telemetry records to local JSONL and optionally
appends them to BigQuery ``${DS_METRICS}.benchmark_runs``.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
from typing import Any

VALID_ENGINE_VARIANTS: frozenset[str] = frozenset(
    {
        "spark_standard",
        "spark_lightning_base",
        "spark_lightning_nqe",
        "bigframes_biglake_gcs",
        "bigframes_native_bq",
    }
)

VALID_STORAGE_BACKENDS: frozenset[str] = frozenset(
    {
        "gcs_parquet",
        "biglake_gcs_parquet",
        "bq_native_capacitor",
    }
)

VALID_STAGE_IDS: frozenset[str] = frozenset(
    {
        "stage1_bronze_to_silver",
        "stage2a_silver_vectorized",
        "stage2b_silver_udf",
        "stage3_silver_to_gold",
    }
)

VALID_SCALE_PRESETS: frozenset[str] = frozenset(
    {
        "small_1g",
        "medium_100g",
        "large_500g",
    }
)

METRIC_SCHEMA_FIELDS: tuple[str, ...] = (
    "run_id",
    "timestamp_utc",
    "scale_preset",
    "engine_variant",
    "storage_backend",
    "stage_id",
    "start_epoch_sec",
    "end_epoch_sec",
    "wall_clock_sec",
    "output_row_count",
    "checksum_net_paid",
    "worker_machine_type",
    "total_worker_vcpus",
    "vcpu_seconds",
    "bq_slot_cap",
    "bq_total_slot_ms",
    "bq_bytes_processed",
    "nqe_enabled",
    "nqe_fallback_note",
    "est_list_cost_usd",
    "sample_fraction",
)

# Parameterized US list rates (us-central1 GCE + Dataproc, US multi-region BigQuery Enterprise).
# Adjust rates in one place if benchmarking in a different region or commitment tier.
US_LIST_RATES: dict[str, Any] = {
    "gce_vcpu_hr_by_family": {
        "n2d-standard": 0.030972,
        "n2-standard": 0.031611,
        "e2-standard": 0.021811,
        "c2-standard": 0.033981,
        "n2d-highmem": 0.030972,
        "n2-highmem": 0.031611,
    },
    "gce_ram_gb_hr_by_family": {
        "n2d-standard": 0.004150,
        "n2-standard": 0.004235,
        "e2-standard": 0.002924,
        "c2-standard": 0.004553,
        "n2d-highmem": 0.004150,
        "n2-highmem": 0.004235,
    },
    "gce_ram_gb_per_vcpu_by_family": {
        "n2d-standard": 4.0,
        "n2-standard": 4.0,
        "e2-standard": 4.0,
        "c2-standard": 4.0,
        "n2d-highmem": 8.0,
        "n2-highmem": 8.0,
    },
    "default_machine_family": "n2d-standard",
    "dataproc_gce_fee_per_vcpu_hr": 0.010,
    "dataproc_lightning_surcharge_per_vcpu_hr": 0.0025,
    "local_ssd_gb_per_disk": 375.0,
    "local_ssd_usd_per_gb_month": 0.080,
    "hours_per_month": 730.0,
    "default_worker_local_ssds": 1,
    "bq_enterprise_slot_hr_usd": 0.060,
}

_DEFAULT_ENV_CONFIG: dict[str, str] = {
    "PROJECT_ID": "",
    "PROJECT_NUMBER": "",
    "BQ_LOCATION": "US",
    "GCS_LOCATION": "US",
    "DATAPROC_REGION": "us-central1",
    "DATAPROC_ZONE": "us-central1-a",
    "BENCH_BUCKET": "",
    "SCALE_PRESET": "small_1g",
    "DATAPROC_IMAGE_VERSION": "2.3-debian12",
    "CLUSTER_STD_NAME": "bench-spark-std-x86",
    "CLUSTER_LGT_NAME": "bench-spark-lightning-x86",
    "MASTER_MACHINE_TYPE": "n2d-standard-4",
    "WORKER_MACHINE_TYPE": "n2d-standard-8",
    "NUM_PRIMARY_WORKERS": "2",
    "NUM_WORKER_LOCAL_SSDS": "1",
    "BQ_ADMIN_PROJECT_ID": "",
    "BQ_RESERVATION_NAME": "bench-elt-slots",
    "BQ_SLOT_CAP": "100",
    "BIGLAKE_CONNECTION_ID": "bench-biglake-us",
    "DS_BRONZE_NATIVE": "bench_bronze_native",
    "DS_BRONZE_BIGLAKE": "bench_bronze_biglake",
    "DS_SILVER_BQ": "bench_silver_bq",
    "DS_GOLD_BQ": "bench_gold_bq",
    "DS_METRICS": "bench_metrics",
    "RUN_PYTHON_UDF_STAGE": "true",
    "UDF_MAX_SAMPLE_ROWS": "500000",
}

_ENV_LINE_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
_VAR_SUB_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def get_repo_root() -> Path:
    """Return the repository root containing ``scripts/``."""
    return Path(__file__).resolve().parent.parent


def _strip_inline_comment(raw_val: str) -> str:
    """Strip unquoted inline comments and matching surrounding quotes."""
    val = raw_val.strip()
    if not val:
        return ""
    if val[0] in ("'", '"'):
        quote = val[0]
        escaped = False
        chars: list[str] = []
        for idx in range(1, len(val)):
            ch = val[idx]
            if quote == '"' and ch == "\\" and not escaped:
                escaped = True
                continue
            if ch == quote and not escaped:
                return "".join(chars)
            chars.append(ch)
            escaped = False
        return "".join(chars)
    # Unquoted value: trim inline '#' comment preceded by whitespace.
    for idx, ch in enumerate(val):
        if ch == "#" and (idx == 0 or val[idx - 1].isspace()):
            return val[:idx].strip()
    return val


def _parse_dotenv_file(path: Path) -> dict[str, str]:
    """Parse a shell-style ``.env`` file using only the Python standard library."""
    parsed: dict[str, str] = {}
    if not path.is_file():
        return parsed
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _ENV_LINE_RE.match(line)
        if not match:
            continue
        key, raw_value = match.groups()
        value = _strip_inline_comment(raw_value)
        # Expand ${VAR} references against previously parsed values and os.environ.
        value = _VAR_SUB_RE.sub(
            lambda m: parsed.get(
                m.group(1) or m.group(2),
                os.environ.get(m.group(1) or m.group(2), ""),
            ),
            value,
        )
        parsed[key] = value
    return parsed


def resolve_bench_config(
    env_path: str | Path | None = None,
    repo_root: str | Path | None = None,
    *,
    populate_environ: bool = True,
) -> dict[str, str]:
    """Load benchmark configuration from ``os.environ``, ``.env``, and defaults.

    Ambient process environment variables take precedence over ``.env`` values,
    which take precedence over default values from ``.env.example``.

    Args:
        env_path: Optional explicit path to a ``.env`` file.
        repo_root: Optional repository root directory. Defaults to parent of ``scripts/``.
        populate_environ: When True, sets resolved non-empty keys in ``os.environ``
            if not already present.

    Returns:
        Dictionary of resolved benchmark configuration strings.
    """
    root = Path(repo_root).resolve() if repo_root else get_repo_root()
    candidate_env = Path(env_path).resolve() if env_path else (root / ".env")
    dotenv_values = _parse_dotenv_file(candidate_env)

    resolved: dict[str, str] = {}
    all_keys = list(_DEFAULT_ENV_CONFIG.keys()) + [
        k for k in dotenv_values if k not in _DEFAULT_ENV_CONFIG
    ]
    for key in all_keys:
        if key in os.environ and os.environ[key] != "":
            resolved[key] = os.environ[key]
        elif key in dotenv_values and dotenv_values[key] != "":
            resolved[key] = dotenv_values[key]
        else:
            resolved[key] = _DEFAULT_ENV_CONFIG.get(key, "")

    if not resolved.get("BQ_ADMIN_PROJECT_ID") and resolved.get("PROJECT_ID"):
        resolved["BQ_ADMIN_PROJECT_ID"] = resolved["PROJECT_ID"]

    if not resolved.get("BENCH_BUCKET") and resolved.get("PROJECT_NUMBER"):
        resolved["BENCH_BUCKET"] = f"lakehouse-bench-{resolved['PROJECT_NUMBER']}"

    try:
        worker_vcpus = infer_vcpus_from_machine_type(resolved["WORKER_MACHINE_TYPE"])
        num_workers = int(resolved["NUM_PRIMARY_WORKERS"])
        resolved["TOTAL_WORKER_VCPUS"] = str(worker_vcpus * num_workers)
    except (KeyError, ValueError):
        resolved.setdefault("TOTAL_WORKER_VCPUS", "16")

    if populate_environ:
        for key, value in resolved.items():
            if value != "" and not os.environ.get(key):
                os.environ[key] = value

    return resolved


def infer_vcpus_from_machine_type(machine_type: str) -> int:
    """Infer vCPU count from a GCE machine type name (e.g. ``n2d-standard-8`` -> 8)."""
    if not machine_type:
        raise ValueError("machine_type must be non-empty")
    custom_match = re.match(r"^(?:[a-z0-9]+-)?custom-(\d+)-\d+$", machine_type)
    if custom_match:
        return int(custom_match.group(1))
    std_match = re.search(r"-(\d+)$", machine_type.strip())
    if std_match:
        return int(std_match.group(1))
    named_defaults = {
        "e2-micro": 2,
        "e2-small": 2,
        "e2-medium": 2,
        "f1-micro": 1,
        "g1-small": 1,
    }
    if machine_type in named_defaults:
        return named_defaults[machine_type]
    raise ValueError(f"Unable to infer vCPU count from machine_type={machine_type!r}")


def _extract_machine_family(machine_type: str | None) -> str:
    """Extract GCE machine family prefix (e.g. ``n2d-standard-8`` -> ``n2d-standard``)."""
    if not machine_type:
        return str(US_LIST_RATES["default_machine_family"])
    parts = machine_type.strip().split("-")
    if len(parts) >= 2:
        candidate = f"{parts[0]}-{parts[1]}"
        if candidate in US_LIST_RATES["gce_vcpu_hr_by_family"]:
            return candidate
    return str(US_LIST_RATES["default_machine_family"])


def estimate_stage_list_cost_usd(
    *,
    engine_variant: str,
    wall_clock_sec: float,
    worker_machine_type: str | None = None,
    total_worker_vcpus: int | None = None,
    bq_slot_cap: int | None = None,
    bq_total_slot_ms: int | None = None,
    num_worker_local_ssds: int | None = None,
) -> float | None:
    """Estimate stage compute list cost in USD using parameterized US list rates.

    For Dataproc Spark variants, prices active worker vCPU-hours (VM vCPU + RAM +
    Dataproc GCE fee + worker NVMe Local SSDs + Lightning Engine surcharge when
    applicable). For BigFrames variants, prices consumed BigQuery Enterprise
    Edition slot-hours (falling back to ``bq_slot_cap * wall_clock_hours`` if
    ``bq_total_slot_ms`` is unavailable).
    """
    if engine_variant in ("spark_standard", "spark_lightning_base", "spark_lightning_nqe"):
        if total_worker_vcpus is None:
            return None
        family = _extract_machine_family(worker_machine_type)
        vcpu_hr_rate = float(US_LIST_RATES["gce_vcpu_hr_by_family"][family])
        ram_gb_per_vcpu = float(US_LIST_RATES["gce_ram_gb_per_vcpu_by_family"][family])
        ram_hr_rate = float(US_LIST_RATES["gce_ram_gb_hr_by_family"][family]) * ram_gb_per_vcpu
        dp_fee = float(US_LIST_RATES["dataproc_gce_fee_per_vcpu_hr"])
        lightning_fee = (
            float(US_LIST_RATES["dataproc_lightning_surcharge_per_vcpu_hr"])
            if engine_variant in ("spark_lightning_base", "spark_lightning_nqe")
            else 0.0
        )

        ssds_per_worker = (
            int(num_worker_local_ssds)
            if num_worker_local_ssds is not None
            else int(US_LIST_RATES["default_worker_local_ssds"])
        )
        ssd_hr_per_disk = (
            float(US_LIST_RATES["local_ssd_gb_per_disk"])
            * float(US_LIST_RATES["local_ssd_usd_per_gb_month"])
            / float(US_LIST_RATES["hours_per_month"])
        )
        per_worker_vcpus = 8
        if worker_machine_type:
            try:
                per_worker_vcpus = max(1, infer_vcpus_from_machine_type(worker_machine_type))
            except ValueError:
                per_worker_vcpus = 8
        ssd_per_vcpu_hr = (ssds_per_worker * ssd_hr_per_disk) / float(per_worker_vcpus)

        worker_vcpu_hours = (float(wall_clock_sec) * float(total_worker_vcpus)) / 3600.0
        total_rate_per_vcpu_hr = vcpu_hr_rate + ram_hr_rate + dp_fee + lightning_fee + ssd_per_vcpu_hr
        return round(worker_vcpu_hours * total_rate_per_vcpu_hr, 6)

    slot_rate = float(US_LIST_RATES["bq_enterprise_slot_hr_usd"])
    if bq_total_slot_ms is not None:
        slot_hours = (float(bq_total_slot_ms) / 1000.0) / 3600.0
        return round(slot_hours * slot_rate, 6)
    if bq_slot_cap is not None:
        ceiling_hours = (float(bq_slot_cap) * float(wall_clock_sec)) / 3600.0
        return round(ceiling_hours * slot_rate, 6)
    return None


def _validate_finite(name: str, value: float) -> float:
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return numeric


def _append_bq_row(record: dict[str, Any], project_id: str, ds_metrics: str) -> None:
    """Append a single telemetry record to ``{project_id}.{ds_metrics}.benchmark_runs``."""
    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-bigquery is required when write_to_bq=True"
        ) from exc

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{ds_metrics}.benchmark_runs"
    errors = client.insert_rows_json(table_id, [record])
    if errors:
        error_text = str(errors)
        # Sandbox/free-tier BigQuery projects disallow streaming inserts; fall back to batch load.
        if "streaming" in error_text.lower() or "free tier" in error_text.lower():
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            load_job = client.load_table_from_json([record], table_id, job_config=job_config)
            load_job.result()
            return
        raise RuntimeError(f"Failed to insert metric row into {table_id}: {errors}")


def log_stage_metric(
    *,
    run_id: str,
    scale_preset: str,
    engine_variant: str,
    storage_backend: str,
    stage_id: str,
    start_epoch_sec: float,
    end_epoch_sec: float,
    output_row_count: int,
    checksum_net_paid: float | None = None,
    worker_machine_type: str | None = None,
    total_worker_vcpus: int | None = None,
    bq_slot_cap: int | None = None,
    bq_total_slot_ms: int | None = None,
    bq_bytes_processed: int | None = None,
    nqe_enabled: bool = False,
    nqe_fallback_note: str = "",
    est_list_cost_usd: float | None = None,
    sample_fraction: float = 1.0,
    jsonl_path: str | Path | None = None,
    write_to_bq: bool = False,
    project_id: str | None = None,
    ds_metrics: str | None = None,
) -> dict[str, Any]:
    """Validate, enrich, and persist a single stage benchmark metric record.

    Writes one JSONL line containing the 21-field benchmark schema (never
    embedding ``project_id``) to ``jsonl_path`` and optionally appends the same
    record to BigQuery ``{project_id}.{ds_metrics}.benchmark_runs``.
    """
    if not run_id or not str(run_id).strip():
        raise ValueError("run_id must be a non-empty string")
    if scale_preset not in VALID_SCALE_PRESETS:
        raise ValueError(
            f"Invalid scale_preset={scale_preset!r}; expected one of {sorted(VALID_SCALE_PRESETS)}"
        )
    if engine_variant not in VALID_ENGINE_VARIANTS:
        raise ValueError(
            f"Invalid engine_variant={engine_variant!r}; expected one of {sorted(VALID_ENGINE_VARIANTS)}"
        )
    if storage_backend not in VALID_STORAGE_BACKENDS:
        raise ValueError(
            f"Invalid storage_backend={storage_backend!r}; expected one of {sorted(VALID_STORAGE_BACKENDS)}"
        )
    if stage_id not in VALID_STAGE_IDS:
        raise ValueError(
            f"Invalid stage_id={stage_id!r}; expected one of {sorted(VALID_STAGE_IDS)}"
        )

    start_val = _validate_finite("start_epoch_sec", start_epoch_sec)
    end_val = _validate_finite("end_epoch_sec", end_epoch_sec)
    if end_val < start_val:
        raise ValueError(
            f"end_epoch_sec ({end_val}) must be >= start_epoch_sec ({start_val})"
        )

    row_count_val = int(output_row_count)
    if row_count_val < 0:
        raise ValueError(f"output_row_count must be >= 0, got {row_count_val}")

    sample_frac_val = _validate_finite("sample_fraction", sample_fraction)
    if sample_frac_val <= 0.0 or sample_frac_val > 1.0:
        raise ValueError(f"sample_fraction must be in (0.0, 1.0], got {sample_frac_val}")

    checksum_val = (
        _validate_finite("checksum_net_paid", checksum_net_paid)
        if checksum_net_paid is not None
        else None
    )

    vcpus_val = int(total_worker_vcpus) if total_worker_vcpus is not None else None
    if vcpus_val is not None and vcpus_val < 0:
        raise ValueError(f"total_worker_vcpus must be >= 0, got {vcpus_val}")

    wall_clock_sec = round(end_val - start_val, 4)
    vcpu_seconds = (
        round(wall_clock_sec * vcpus_val, 4) if vcpus_val is not None else None
    )
    timestamp_utc = datetime.fromtimestamp(end_val, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    resolved_cost = (
        _validate_finite("est_list_cost_usd", est_list_cost_usd)
        if est_list_cost_usd is not None
        else estimate_stage_list_cost_usd(
            engine_variant=engine_variant,
            wall_clock_sec=wall_clock_sec,
            worker_machine_type=worker_machine_type,
            total_worker_vcpus=vcpus_val,
            bq_slot_cap=int(bq_slot_cap) if bq_slot_cap is not None else None,
            bq_total_slot_ms=int(bq_total_slot_ms) if bq_total_slot_ms is not None else None,
        )
    )

    record: dict[str, Any] = {
        "run_id": str(run_id).strip(),
        "timestamp_utc": timestamp_utc,
        "scale_preset": scale_preset,
        "engine_variant": engine_variant,
        "storage_backend": storage_backend,
        "stage_id": stage_id,
        "start_epoch_sec": round(start_val, 4),
        "end_epoch_sec": round(end_val, 4),
        "wall_clock_sec": wall_clock_sec,
        "output_row_count": row_count_val,
        "checksum_net_paid": checksum_val,
        "worker_machine_type": worker_machine_type,
        "total_worker_vcpus": vcpus_val,
        "vcpu_seconds": vcpu_seconds,
        "bq_slot_cap": int(bq_slot_cap) if bq_slot_cap is not None else None,
        "bq_total_slot_ms": int(bq_total_slot_ms) if bq_total_slot_ms is not None else None,
        "bq_bytes_processed": int(bq_bytes_processed) if bq_bytes_processed is not None else None,
        "nqe_enabled": bool(nqe_enabled),
        "nqe_fallback_note": str(nqe_fallback_note or ""),
        "est_list_cost_usd": resolved_cost,
        "sample_fraction": round(sample_frac_val, 6),
    }

    target_path = (
        Path(jsonl_path)
        if jsonl_path is not None
        else get_repo_root() / "results" / "live_runs" / "benchmark_runs.jsonl"
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=False) + "\n")
        fh.flush()

    if write_to_bq:
        cfg = resolve_bench_config(populate_environ=False)
        target_project = (project_id or cfg.get("PROJECT_ID", "")).strip()
        target_dataset = (ds_metrics or cfg.get("DS_METRICS", "bench_metrics")).strip()
        if not target_project or target_project == "your-gcp-project-id":
            raise ValueError(
                "Valid project_id (or PROJECT_ID env var) is required when write_to_bq=True"
            )
        _append_bq_row(record, target_project, target_dataset)

    return record


def _read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.is_file():
        return records
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw_line in enumerate(fh, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_no} of {path}: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"Expected JSON object on line {line_no} of {path}")
            records.append(parsed)
    return records


def load_benchmark_runs(
    live_path: str | Path | None = None,
    sample_path: str | Path | None = None,
    repo_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Load benchmark telemetry records, preferring live runs and falling back to sample data.

    Search order when explicit paths are omitted:
      1. ``<repo_root>/results/live_runs/benchmark_runs.jsonl``
      2. ``<repo_root>/results/benchmark_runs.jsonl``
      3. ``<repo_root>/results/sample_benchmark_runs.jsonl``
    """
    root = Path(repo_root).resolve() if repo_root else get_repo_root()
    candidates: list[Path] = []
    if live_path is not None:
        candidates.append(Path(live_path))
    else:
        candidates.append(root / "results" / "live_runs" / "benchmark_runs.jsonl")
        candidates.append(root / "results" / "benchmark_runs.jsonl")

    if sample_path is not None:
        candidates.append(Path(sample_path))
    else:
        candidates.append(root / "results" / "sample_benchmark_runs.jsonl")

    for candidate in candidates:
        records = _read_jsonl_records(candidate)
        if records:
            return records
    return []
