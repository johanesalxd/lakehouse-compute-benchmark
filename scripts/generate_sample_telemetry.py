#!/usr/bin/env python3
"""Deterministic generator and validator for offline sample benchmark telemetry.

Generates a realistic 20-record ``small_1g`` dataset (5 engine variants x 4 stages)
matching the exact 21-field schema in ``SPEC.md`` §4 (including ``sample_fraction``)
and writes it to ``results/sample_benchmark_runs.jsonl``.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PATH = REPO_ROOT / "results" / "sample_benchmark_runs.jsonl"

RUN_ID_DEFAULT = "sample_dryrun_small_1g"
SCALE_PRESET_DEFAULT = "small_1g"

EXPECTED_FIELDS: tuple[str, ...] = (
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

STAGE_ORDER: tuple[str, ...] = (
    "stage1_bronze_to_silver",
    "stage2a_silver_vectorized",
    "stage2b_silver_udf",
    "stage3_silver_to_gold",
)

ENGINE_SPECS: dict[str, dict[str, Any]] = {
    "spark_standard": {
        "storage_backend": "gcs_parquet",
        "worker_machine_type": "n2d-standard-8",
        "total_worker_vcpus": 16,
        "bq_slot_cap": None,
        "nqe_enabled": False,
    },
    "spark_lightning_base": {
        "storage_backend": "gcs_parquet",
        "worker_machine_type": "n2d-standard-8",
        "total_worker_vcpus": 16,
        "bq_slot_cap": None,
        "nqe_enabled": False,
    },
    "spark_lightning_nqe": {
        "storage_backend": "gcs_parquet",
        "worker_machine_type": "n2d-standard-8",
        "total_worker_vcpus": 16,
        "bq_slot_cap": None,
        "nqe_enabled": True,
    },
    "bigframes_biglake_gcs": {
        "storage_backend": "biglake_gcs_parquet",
        "worker_machine_type": None,
        "total_worker_vcpus": None,
        "bq_slot_cap": 100,
        "nqe_enabled": False,
    },
    "bigframes_native_bq": {
        "storage_backend": "bq_native_capacitor",
        "worker_machine_type": None,
        "total_worker_vcpus": None,
        "bq_slot_cap": 100,
        "nqe_enabled": False,
    },
}

# Reference TPC-DS 1G Medallion outputs (100% row-count parity across all variants)
STAGE_OUTPUT_PARITY: dict[str, dict[str, float | int]] = {
    "stage1_bronze_to_silver": {
        "output_row_count": 3_436_840,
        "base_checksum_net_paid": 1_842_654_321.40,
    },
    "stage2a_silver_vectorized": {
        "output_row_count": 3_436_840,
        "base_checksum_net_paid": 1_842_654_321.40,
    },
    "stage2b_silver_udf": {
        "output_row_count": 3_436_840,
        "base_checksum_net_paid": 1_842_654_321.40,
    },
    "stage3_silver_to_gold": {
        "output_row_count": 248_620,
        "base_checksum_net_paid": 1_811_420_985.20,
    },
}

# Realistic us-central1 / US list parameters for est_list_cost_usd (matches metrics_helper.py)
SPARK_STD_LIST_USD_PER_VCPU_HR = (
    0.030972 + (4.0 * 0.004150) + 0.010000 + ((375.0 * 0.080 / 730.0) / 8.0)
)
LIGHTNING_SURCHARGE_USD_PER_VCPU_HR = 0.002500
BQ_ENTERPRISE_PAYGO_USD_PER_SLOT_HR = 0.060000

# Baseline realistic small_1g profiles per (engine_variant, stage_id)
STAGE_PROFILES: dict[str, dict[str, dict[str, Any]]] = {
    "spark_standard": {
        "stage1_bronze_to_silver": {"wall_sec": 28.40},
        "stage2a_silver_vectorized": {"wall_sec": 11.85},
        "stage2b_silver_udf": {"wall_sec": 54.60},
        "stage3_silver_to_gold": {"wall_sec": 39.20},
    },
    "spark_lightning_base": {
        "stage1_bronze_to_silver": {"wall_sec": 24.10},
        "stage2a_silver_vectorized": {"wall_sec": 10.20},
        "stage2b_silver_udf": {"wall_sec": 51.80},
        "stage3_silver_to_gold": {"wall_sec": 33.40},
    },
    "spark_lightning_nqe": {
        "stage1_bronze_to_silver": {"wall_sec": 12.60},
        "stage2a_silver_vectorized": {"wall_sec": 4.45},
        "stage2b_silver_udf": {
            "wall_sec": 56.20,
            "nqe_fallback_note": (
                "VeloxColumnarToRow -> BatchEvalPythonExec -> RowToVeloxColumnar "
                "(scalar PySpark @F.udf falls back to JVM/Python workers)"
            ),
        },
        "stage3_silver_to_gold": {"wall_sec": 16.80},
    },
    "bigframes_biglake_gcs": {
        "stage1_bronze_to_silver": {
            "wall_sec": 13.40,
            "slot_ms": 512_000,
            "bytes_processed": 428_500_120,
        },
        "stage2a_silver_vectorized": {
            "wall_sec": 5.80,
            "slot_ms": 148_000,
            "bytes_processed": 312_400_500,
        },
        "stage2b_silver_udf": {
            "wall_sec": 7.90,
            "slot_ms": 235_000,
            "bytes_processed": 312_400_500,
        },
        "stage3_silver_to_gold": {
            "wall_sec": 15.10,
            "slot_ms": 645_000,
            "bytes_processed": 486_200_800,
        },
    },
    "bigframes_native_bq": {
        "stage1_bronze_to_silver": {
            "wall_sec": 9.20,
            "slot_ms": 342_000,
            "bytes_processed": 248_900_400,
        },
        "stage2a_silver_vectorized": {
            "wall_sec": 4.30,
            "slot_ms": 112_000,
            "bytes_processed": 196_400_200,
        },
        "stage2b_silver_udf": {
            "wall_sec": 6.10,
            "slot_ms": 184_000,
            "bytes_processed": 196_400_200,
        },
        "stage3_silver_to_gold": {
            "wall_sec": 10.80,
            "slot_ms": 438_000,
            "bytes_processed": 284_100_600,
        },
    },
}


def _epoch_to_utc_iso(epoch_sec: float) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_sample_records(seed: int = 42, run_id: str = RUN_ID_DEFAULT) -> list[dict[str, Any]]:
    """Build deterministic 20-record sample telemetry for ``small_1g``."""
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []

    # Fixed generic UTC anchor: 2026-11-16T08:00:00Z
    base_epoch = 1794816000.0
    engine_offset = 0.0

    for engine_variant, spec in ENGINE_SPECS.items():
        cursor_epoch = base_epoch + engine_offset
        is_spark = spec["total_worker_vcpus"] is not None

        for stage_id in STAGE_ORDER:
            profile = STAGE_PROFILES[engine_variant][stage_id]
            parity = STAGE_OUTPUT_PARITY[stage_id]

            # Deterministic subtle runtime jitter (+/- 1.5%)
            jitter_factor = 1.0 + rng.uniform(-0.015, 0.015)
            wall_clock_sec = round(profile["wall_sec"] * jitter_factor, 2)
            start_epoch_sec = round(cursor_epoch, 2)
            end_epoch_sec = round(start_epoch_sec + wall_clock_sec, 2)
            wall_clock_exact = round(end_epoch_sec - start_epoch_sec, 4)

            # Deterministic IEEE-754 summation drift within +/- 0.0001% (1e-6)
            base_checksum = float(parity["base_checksum_net_paid"])
            if engine_variant == "spark_standard":
                checksum_net_paid = round(base_checksum, 2)
            else:
                drift_ratio = rng.uniform(-8e-7, 8e-7)
                checksum_net_paid = round(base_checksum * (1.0 + drift_ratio), 2)

            if is_spark:
                vcpus = int(spec["total_worker_vcpus"])
                vcpu_seconds = round(wall_clock_exact * vcpus, 4)
                rate = SPARK_STD_LIST_USD_PER_VCPU_HR + (
                    LIGHTNING_SURCHARGE_USD_PER_VCPU_HR
                    if engine_variant in ("spark_lightning_base", "spark_lightning_nqe")
                    else 0.0
                )
                est_list_cost_usd = round((vcpu_seconds / 3600.0) * rate, 6)
                bq_total_slot_ms = None
                bq_bytes_processed = None
            else:
                vcpu_seconds = None
                bq_total_slot_ms = int(round(profile["slot_ms"] * jitter_factor))
                bq_bytes_processed = int(profile["bytes_processed"])
                est_list_cost_usd = round(
                    (bq_total_slot_ms / 3_600_000.0) * BQ_ENTERPRISE_PAYGO_USD_PER_SLOT_HR,
                    6,
                )

            record: dict[str, Any] = {
                "run_id": run_id,
                "timestamp_utc": _epoch_to_utc_iso(end_epoch_sec),
                "scale_preset": SCALE_PRESET_DEFAULT,
                "engine_variant": engine_variant,
                "storage_backend": spec["storage_backend"],
                "stage_id": stage_id,
                "start_epoch_sec": start_epoch_sec,
                "end_epoch_sec": end_epoch_sec,
                "wall_clock_sec": wall_clock_exact,
                "output_row_count": int(parity["output_row_count"]),
                "checksum_net_paid": checksum_net_paid,
                "worker_machine_type": spec["worker_machine_type"],
                "total_worker_vcpus": spec["total_worker_vcpus"],
                "vcpu_seconds": vcpu_seconds,
                "bq_slot_cap": spec["bq_slot_cap"],
                "bq_total_slot_ms": bq_total_slot_ms,
                "bq_bytes_processed": bq_bytes_processed,
                "nqe_enabled": bool(spec["nqe_enabled"]),
                "nqe_fallback_note": str(profile.get("nqe_fallback_note", "")),
                "est_list_cost_usd": est_list_cost_usd,
                "sample_fraction": 1.0,
            }
            records.append(record)

            # Inter-stage outside-timer validation & checksum query pause (~1.2s - 2.4s)
            cursor_epoch = end_epoch_sec + round(rng.uniform(1.2, 2.4), 2)

        # Gap between engine benchmark runs (~180s)
        engine_offset += 180.0

    return records


def validate_records(records: list[dict[str, Any]]) -> None:
    """Validate schema, types, invariants, and cross-engine parity (fail closed)."""
    expected_count = len(ENGINE_SPECS) * len(STAGE_ORDER)
    if len(records) != expected_count:
        raise ValueError(f"Expected {expected_count} records, got {len(records)}")

    seen_pairs: set[tuple[str, str]] = set()
    by_stage: dict[str, list[dict[str, Any]]] = {stage: [] for stage in STAGE_ORDER}

    for idx, rec in enumerate(records, start=1):
        keys = tuple(rec.keys())
        if keys != EXPECTED_FIELDS:
            missing = set(EXPECTED_FIELDS) - set(keys)
            extra = set(keys) - set(EXPECTED_FIELDS)
            raise ValueError(f"Record {idx} field mismatch: missing={missing}, extra={extra}")

        engine = rec["engine_variant"]
        stage = rec["stage_id"]
        if engine not in ENGINE_SPECS:
            raise ValueError(f"Record {idx} invalid engine_variant: {engine}")
        if stage not in STAGE_ORDER:
            raise ValueError(f"Record {idx} invalid stage_id: {stage}")
        if rec["storage_backend"] != ENGINE_SPECS[engine]["storage_backend"]:
            raise ValueError(f"Record {idx} storage_backend mismatch for {engine}")
        if (engine, stage) in seen_pairs:
            raise ValueError(f"Duplicate (engine_variant, stage_id): {(engine, stage)}")
        seen_pairs.add((engine, stage))

        expected_wall = round(rec["end_epoch_sec"] - rec["start_epoch_sec"], 4)
        if abs(rec["wall_clock_sec"] - expected_wall) > 1e-4 or rec["wall_clock_sec"] <= 0:
            raise ValueError(f"Record {idx} invalid wall_clock_sec: {rec['wall_clock_sec']}")
        if rec["sample_fraction"] != 1.0:
            raise ValueError(f"Record {idx} sample_fraction must be 1.0 on small_1g")

        if engine.startswith("spark_"):
            if rec["worker_machine_type"] != "n2d-standard-8" or rec["total_worker_vcpus"] != 16:
                raise ValueError(f"Record {idx} invalid Spark hardware metadata")
            expected_vcpu_s = round(rec["wall_clock_sec"] * rec["total_worker_vcpus"], 4)
            if abs(rec["vcpu_seconds"] - expected_vcpu_s) > 1e-3:
                raise ValueError(f"Record {idx} vcpu_seconds mismatch")
            if any(rec[k] is not None for k in ("bq_slot_cap", "bq_total_slot_ms", "bq_bytes_processed")):
                raise ValueError(f"Record {idx} Spark row must keep BQ fields null")
        else:
            if rec["bq_slot_cap"] != 100 or not rec["bq_total_slot_ms"] or not rec["bq_bytes_processed"]:
                raise ValueError(f"Record {idx} missing BigQuery telemetry")
            if any(rec[k] is not None for k in ("worker_machine_type", "total_worker_vcpus", "vcpu_seconds")):
                raise ValueError(f"Record {idx} BigFrames row must keep Spark fields null")

        if engine == "spark_lightning_nqe":
            if not rec["nqe_enabled"]:
                raise ValueError(f"Record {idx} spark_lightning_nqe must set nqe_enabled=True")
            if stage == "stage2b_silver_udf" and not rec["nqe_fallback_note"]:
                raise ValueError(f"Record {idx} must populate nqe_fallback_note on stage2b_silver_udf")
        elif rec["nqe_enabled"]:
            raise ValueError(f"Record {idx} nqe_enabled must be False for {engine}")

        by_stage[stage].append(rec)

    # Cross-engine row-count 100% parity & checksum within +/- 0.0001%
    for stage, stage_rows in by_stage.items():
        row_counts = {r["output_row_count"] for r in stage_rows}
        if len(row_counts) != 1:
            raise ValueError(f"Stage {stage} row count parity failed: {row_counts}")

        ref_checksum = stage_rows[0]["checksum_net_paid"]
        for r in stage_rows:
            delta_pct = abs(r["checksum_net_paid"] - ref_checksum) / ref_checksum * 100.0
            if delta_pct > 0.0001:
                raise ValueError(
                    f"Stage {stage} checksum drift {delta_pct:.6f}% exceeds +/-0.0001% for {r['engine_variant']}"
                )


def write_jsonl(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write validated records as UTF-8 JSONL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")


def load_jsonl(input_path: Path) -> list[dict[str, Any]]:
    """Read JSONL file into list of dictionaries."""
    with input_path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and validate sample benchmark telemetry JSONL.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic PRNG seed (default: 42)")
    parser.add_argument("--run-id", type=str, default=RUN_ID_DEFAULT, help="Run identifier tag")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Destination JSONL path (default: results/sample_benchmark_runs.jsonl)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate an existing JSONL file without rewriting it",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.validate_only:
        records = load_jsonl(args.output)
        validate_records(records)
        print(f"VALIDATED {len(records)} records in {args.output}")
        return 0

    records = generate_sample_records(seed=args.seed, run_id=args.run_id)
    validate_records(records)
    write_jsonl(records, args.output)
    reloaded = load_jsonl(args.output)
    validate_records(reloaded)
    print(f"GENERATED & VALIDATED {len(reloaded)} records (seed={args.seed}, run_id={args.run_id}) -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
