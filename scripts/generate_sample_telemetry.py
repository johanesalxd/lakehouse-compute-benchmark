#!/usr/bin/env python3
"""Deterministic generator and validator for offline 3-scale benchmark telemetry.

Generates a realistic 60-record multi-scale dataset across ``small_1g``,
``medium_100g``, and ``large_500g`` (3 scales x 5 engine variants x 4 stages)
matching the exact 21-field schema in ``SPEC.md`` §4 (including
``sample_fraction=0.05`` on Stage 2B for ``medium_100g`` and ``large_500g``)
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

RUN_ID_PREFIX_DEFAULT = "sample_dryrun"

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

SCALE_ORDER: tuple[str, ...] = (
    "small_1g",
    "medium_100g",
    "large_500g",
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
        "is_spark": True,
        "nqe_enabled": False,
    },
    "spark_lightning_base": {
        "storage_backend": "gcs_parquet",
        "is_spark": True,
        "nqe_enabled": False,
    },
    "spark_lightning_nqe": {
        "storage_backend": "gcs_parquet",
        "is_spark": True,
        "nqe_enabled": True,
    },
    "bigframes_biglake_gcs": {
        "storage_backend": "biglake_gcs_parquet",
        "is_spark": False,
        "nqe_enabled": False,
    },
    "bigframes_native_bq": {
        "storage_backend": "bq_native_capacitor",
        "is_spark": False,
        "nqe_enabled": False,
    },
}

# Realistic us-central1 / US list parameters for est_list_cost_usd (matches metrics_helper.py)
US_N2D_VCPU_HR = 0.030972
US_N2D_RAM_GB_HR = 0.004150
US_N2D_RAM_GB_PER_VCPU = 4.0
DATAPROC_GCE_FEE_PER_VCPU_HR = 0.010000
LIGHTNING_SURCHARGE_USD_PER_VCPU_HR = 0.002500
LOCAL_SSD_GB_PER_DISK = 375.0
LOCAL_SSD_USD_PER_GB_MONTH = 0.080000
HOURS_PER_MONTH = 730.0
BQ_ENTERPRISE_PAYGO_USD_PER_SLOT_HR = 0.060000

NQE_FALLBACK_NOTE_UDF = (
    "VeloxColumnarToRow -> BatchEvalPythonExec -> RowToVeloxColumnar "
    "(scalar PySpark @F.udf falls back to JVM/Python workers)"
)

# Per-scale hardware profiles, parity ground truth, and engine stage latency profiles
SCALE_PROFILES: dict[str, dict[str, Any]] = {
    "small_1g": {
        "anchor_epoch": 1794816000.0,  # 2026-11-16T08:00:00Z
        "hardware": {
            "worker_machine_type": "n2d-standard-8",
            "vcpus_per_worker": 8,
            "total_worker_vcpus": 16,
            "ssds_per_worker": 1,
            "bq_slot_cap": 100,
        },
        "stage_parity": {
            "stage1_bronze_to_silver": {
                "output_row_count": 3_372_059,
                "base_checksum_net_paid": 5_854_973_210.40,
                "sample_fraction": 1.0,
            },
            "stage2a_silver_vectorized": {
                "output_row_count": 3_372_059,
                "base_checksum_net_paid": 5_854_973_210.40,
                "sample_fraction": 1.0,
            },
            "stage2b_silver_udf": {
                "output_row_count": 3_372_059,
                "base_checksum_net_paid": 5_854_973_210.40,
                "sample_fraction": 1.0,
            },
            "stage3_silver_to_gold": {
                "output_row_count": 424_486,
                "base_checksum_net_paid": 5_839_057_120.80,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 16.20},
                "stage2a_silver_vectorized": {"wall_sec": 7.40},
                "stage2b_silver_udf": {"wall_sec": 48.50},
                "stage3_silver_to_gold": {"wall_sec": 17.80},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 15.40},
                "stage2a_silver_vectorized": {"wall_sec": 6.90},
                "stage2b_silver_udf": {"wall_sec": 46.20},
                "stage3_silver_to_gold": {"wall_sec": 17.10},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 11.20},
                "stage2a_silver_vectorized": {"wall_sec": 3.80},
                "stage2b_silver_udf": {
                    "wall_sec": 52.40,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 14.20},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 15.50,
                    "slot_ms": 145_000,
                    "bytes_processed": 428_500_120,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 10.20,
                    "slot_ms": 48_000,
                    "bytes_processed": 312_400_500,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 14.50,
                    "slot_ms": 85_000,
                    "bytes_processed": 312_400_500,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 18.60,
                    "slot_ms": 185_000,
                    "bytes_processed": 486_200_800,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 14.80,
                    "slot_ms": 92_000,
                    "bytes_processed": 248_900_400,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 9.80,
                    "slot_ms": 36_000,
                    "bytes_processed": 196_400_200,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 12.80,
                    "slot_ms": 64_000,
                    "bytes_processed": 196_400_200,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 17.90,
                    "slot_ms": 124_000,
                    "bytes_processed": 284_100_600,
                },
            },
        },
    },
    "medium_100g": {
        "anchor_epoch": 1794819600.0,  # 2026-11-16T09:00:00Z
        "hardware": {
            "worker_machine_type": "n2d-standard-16",
            "vcpus_per_worker": 16,
            "total_worker_vcpus": 64,
            "ssds_per_worker": 2,
            "bq_slot_cap": 200,
        },
        "stage_parity": {
            "stage1_bronze_to_silver": {
                "output_row_count": 142_850_400,
                "base_checksum_net_paid": 268_420_510_800.00,
                "sample_fraction": 1.0,
            },
            "stage2a_silver_vectorized": {
                "output_row_count": 142_850_400,
                "base_checksum_net_paid": 268_420_510_800.00,
                "sample_fraction": 1.0,
            },
            "stage2b_silver_udf": {
                "output_row_count": 7_142_520,
                "base_checksum_net_paid": 13_421_025_540.00,
                "sample_fraction": 0.05,
            },
            "stage3_silver_to_gold": {
                "output_row_count": 16_420_800,
                "base_checksum_net_paid": 267_910_240_120.00,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 168.00},
                "stage2a_silver_vectorized": {"wall_sec": 74.00},
                "stage2b_silver_udf": {"wall_sec": 52.00},
                "stage3_silver_to_gold": {"wall_sec": 196.00},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 146.00},
                "stage2a_silver_vectorized": {"wall_sec": 64.00},
                "stage2b_silver_udf": {"wall_sec": 49.50},
                "stage3_silver_to_gold": {"wall_sec": 168.00},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 78.00},
                "stage2a_silver_vectorized": {"wall_sec": 28.50},
                "stage2b_silver_udf": {
                    "wall_sec": 56.40,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 88.50},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 86.00,
                    "slot_ms": 6_200_000,
                    "bytes_processed": 41_850_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 34.00,
                    "slot_ms": 1_850_000,
                    "bytes_processed": 29_400_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 24.50,
                    "slot_ms": 1_350_000,
                    "bytes_processed": 29_400_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 98.00,
                    "slot_ms": 7_900_000,
                    "bytes_processed": 46_200_000_000,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 56.00,
                    "slot_ms": 3_650_000,
                    "bytes_processed": 23_900_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 24.50,
                    "slot_ms": 1_350_000,
                    "bytes_processed": 18_400_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 19.20,
                    "slot_ms": 980_000,
                    "bytes_processed": 18_400_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 66.50,
                    "slot_ms": 5_100_000,
                    "bytes_processed": 27_100_000_000,
                },
            },
        },
    },
    "large_500g": {
        "anchor_epoch": 1794826800.0,  # 2026-11-16T11:00:00Z
        "hardware": {
            "worker_machine_type": "n2d-standard-16",
            "vcpus_per_worker": 16,
            "total_worker_vcpus": 64,
            "ssds_per_worker": 2,
            "bq_slot_cap": 200,
        },
        "stage_parity": {
            "stage1_bronze_to_silver": {
                "output_row_count": 714_252_000,
                "base_checksum_net_paid": 1_342_102_554_000.00,
                "sample_fraction": 1.0,
            },
            "stage2a_silver_vectorized": {
                "output_row_count": 714_252_000,
                "base_checksum_net_paid": 1_342_102_554_000.00,
                "sample_fraction": 1.0,
            },
            "stage2b_silver_udf": {
                "output_row_count": 35_712_600,
                "base_checksum_net_paid": 67_105_127_700.00,
                "sample_fraction": 0.05,
            },
            "stage3_silver_to_gold": {
                "output_row_count": 78_640_500,
                "base_checksum_net_paid": 1_339_551_200_600.00,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 845.00},
                "stage2a_silver_vectorized": {"wall_sec": 365.00},
                "stage2b_silver_udf": {"wall_sec": 248.00},
                "stage3_silver_to_gold": {"wall_sec": 1060.00},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 725.00},
                "stage2a_silver_vectorized": {"wall_sec": 312.00},
                "stage2b_silver_udf": {"wall_sec": 236.00},
                "stage3_silver_to_gold": {"wall_sec": 895.00},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 372.00},
                "stage2a_silver_vectorized": {"wall_sec": 132.00},
                "stage2b_silver_udf": {
                    "wall_sec": 268.00,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 442.00},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 358.00,
                    "slot_ms": 30_500_000,
                    "bytes_processed": 209_250_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 114.00,
                    "slot_ms": 8_800_000,
                    "bytes_processed": 147_000_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 84.00,
                    "slot_ms": 6_400_000,
                    "bytes_processed": 147_000_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 422.00,
                    "slot_ms": 38_200_000,
                    "bytes_processed": 231_000_000_000,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 218.00,
                    "slot_ms": 17_800_000,
                    "bytes_processed": 119_500_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 78.00,
                    "slot_ms": 6_200_000,
                    "bytes_processed": 92_000_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 62.00,
                    "slot_ms": 4_400_000,
                    "bytes_processed": 92_000_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 264.00,
                    "slot_ms": 24_400_000,
                    "bytes_processed": 135_500_000_000,
                },
            },
        },
    },
}


def _epoch_to_utc_iso(epoch_sec: float) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _spark_list_usd_per_vcpu_hr(
    vcpus_per_worker: int,
    ssds_per_worker: int,
    lightning_enabled: bool,
) -> float:
    local_ssd_per_vcpu_hr = (
        ssds_per_worker * LOCAL_SSD_GB_PER_DISK * LOCAL_SSD_USD_PER_GB_MONTH / HOURS_PER_MONTH
    ) / float(vcpus_per_worker)
    base_rate = (
        US_N2D_VCPU_HR
        + (US_N2D_RAM_GB_PER_VCPU * US_N2D_RAM_GB_HR)
        + DATAPROC_GCE_FEE_PER_VCPU_HR
        + local_ssd_per_vcpu_hr
    )
    return base_rate + (LIGHTNING_SURCHARGE_USD_PER_VCPU_HR if lightning_enabled else 0.0)


def generate_sample_records(
    seed: int = 42,
    run_id_prefix: str = RUN_ID_PREFIX_DEFAULT,
) -> list[dict[str, Any]]:
    """Build deterministic 60-record 3-scale telemetry (small_1g, medium_100g, large_500g)."""
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []

    for scale_preset in SCALE_ORDER:
        scale_cfg = SCALE_PROFILES[scale_preset]
        hw = scale_cfg["hardware"]
        stage_parity = scale_cfg["stage_parity"]
        stage_profiles = scale_cfg["stage_profiles"]
        run_id = f"{run_id_prefix}_{scale_preset}"

        base_epoch = float(scale_cfg["anchor_epoch"])
        engine_offset = 0.0

        for engine_variant, spec in ENGINE_SPECS.items():
            cursor_epoch = base_epoch + engine_offset
            is_spark = bool(spec["is_spark"])
            engine_gap = 180.0 if scale_preset == "small_1g" else 600.0

            for stage_id in STAGE_ORDER:
                profile = stage_profiles[engine_variant][stage_id]
                parity = stage_parity[stage_id]

                jitter_factor = 1.0 + rng.uniform(-0.015, 0.015)
                wall_clock_sec = round(profile["wall_sec"] * jitter_factor, 2)
                start_epoch_sec = round(cursor_epoch, 2)
                end_epoch_sec = round(start_epoch_sec + wall_clock_sec, 2)
                wall_clock_exact = round(end_epoch_sec - start_epoch_sec, 4)

                base_checksum = float(parity["base_checksum_net_paid"])
                if engine_variant == "spark_standard":
                    checksum_net_paid = round(base_checksum, 2)
                else:
                    drift_ratio = rng.uniform(-8e-7, 8e-7)
                    checksum_net_paid = round(base_checksum * (1.0 + drift_ratio), 2)

                if is_spark:
                    worker_machine_type: str | None = str(hw["worker_machine_type"])
                    vcpus = int(hw["total_worker_vcpus"])
                    total_worker_vcpus: int | None = vcpus
                    vcpu_seconds: float | None = round(wall_clock_exact * vcpus, 4)
                    rate = _spark_list_usd_per_vcpu_hr(
                        vcpus_per_worker=int(hw["vcpus_per_worker"]),
                        ssds_per_worker=int(hw["ssds_per_worker"]),
                        lightning_enabled=engine_variant in ("spark_lightning_base", "spark_lightning_nqe"),
                    )
                    est_list_cost_usd = round((vcpu_seconds / 3600.0) * rate, 6)
                    bq_slot_cap: int | None = None
                    bq_total_slot_ms: int | None = None
                    bq_bytes_processed: int | None = None
                else:
                    worker_machine_type = None
                    total_worker_vcpus = None
                    vcpu_seconds = None
                    bq_slot_cap = int(hw["bq_slot_cap"])
                    bq_total_slot_ms = int(round(profile["slot_ms"] * jitter_factor))
                    bq_bytes_processed = int(profile["bytes_processed"])
                    est_list_cost_usd = round(
                        (bq_total_slot_ms / 3_600_000.0) * BQ_ENTERPRISE_PAYGO_USD_PER_SLOT_HR,
                        6,
                    )

                record: dict[str, Any] = {
                    "run_id": run_id,
                    "timestamp_utc": _epoch_to_utc_iso(end_epoch_sec),
                    "scale_preset": scale_preset,
                    "engine_variant": engine_variant,
                    "storage_backend": spec["storage_backend"],
                    "stage_id": stage_id,
                    "start_epoch_sec": start_epoch_sec,
                    "end_epoch_sec": end_epoch_sec,
                    "wall_clock_sec": wall_clock_exact,
                    "output_row_count": int(parity["output_row_count"]),
                    "checksum_net_paid": checksum_net_paid,
                    "worker_machine_type": worker_machine_type,
                    "total_worker_vcpus": total_worker_vcpus,
                    "vcpu_seconds": vcpu_seconds,
                    "bq_slot_cap": bq_slot_cap,
                    "bq_total_slot_ms": bq_total_slot_ms,
                    "bq_bytes_processed": bq_bytes_processed,
                    "nqe_enabled": bool(spec["nqe_enabled"]),
                    "nqe_fallback_note": str(profile.get("nqe_fallback_note", "")),
                    "est_list_cost_usd": est_list_cost_usd,
                    "sample_fraction": float(parity["sample_fraction"]),
                }
                records.append(record)

                cursor_epoch = end_epoch_sec + round(rng.uniform(1.2, 2.4), 2)

            engine_offset += engine_gap

    return records


def validate_records(records: list[dict[str, Any]]) -> None:
    """Validate schema, per-scale hardware/sample invariants, and cross-engine parity."""
    expected_count = len(SCALE_ORDER) * len(ENGINE_SPECS) * len(STAGE_ORDER)
    if len(records) != expected_count:
        raise ValueError(f"Expected {expected_count} records across {len(SCALE_ORDER)} scales, got {len(records)}")

    seen_triples: set[tuple[str, str, str]] = set()
    by_scale_stage: dict[tuple[str, str], list[dict[str, Any]]] = {
        (scale, stage): [] for scale in SCALE_ORDER for stage in STAGE_ORDER
    }

    for idx, rec in enumerate(records):
        if tuple(rec.keys()) != EXPECTED_FIELDS:
            missing = set(EXPECTED_FIELDS) - set(rec.keys())
            extra = set(rec.keys()) - set(EXPECTED_FIELDS)
            raise ValueError(f"Record {idx} schema mismatch: missing={missing}, extra={extra}, keys={list(rec.keys())}")

        scale = rec["scale_preset"]
        engine = rec["engine_variant"]
        stage = rec["stage_id"]
        if scale not in SCALE_PROFILES:
            raise ValueError(f"Record {idx} unknown scale_preset: {scale}")
        if engine not in ENGINE_SPECS:
            raise ValueError(f"Record {idx} unknown engine_variant: {engine}")
        if stage not in STAGE_ORDER:
            raise ValueError(f"Record {idx} unknown stage_id: {stage}")
        if (scale, engine, stage) in seen_triples:
            raise ValueError(f"Duplicate (scale_preset, engine_variant, stage_id): {(scale, engine, stage)}")
        seen_triples.add((scale, engine, stage))

        expected_wall = round(rec["end_epoch_sec"] - rec["start_epoch_sec"], 4)
        if abs(rec["wall_clock_sec"] - expected_wall) > 1e-4 or rec["wall_clock_sec"] <= 0:
            raise ValueError(f"Record {idx} invalid wall_clock_sec: {rec['wall_clock_sec']}")

        hw = SCALE_PROFILES[scale]["hardware"]
        expected_fraction = SCALE_PROFILES[scale]["stage_parity"][stage]["sample_fraction"]
        if abs(float(rec["sample_fraction"]) - float(expected_fraction)) > 1e-9:
            raise ValueError(
                f"Record {idx} sample_fraction={rec['sample_fraction']} != expected {expected_fraction} for {scale}/{stage}"
            )

        if engine.startswith("spark_"):
            if (
                rec["worker_machine_type"] != hw["worker_machine_type"]
                or rec["total_worker_vcpus"] != hw["total_worker_vcpus"]
            ):
                raise ValueError(f"Record {idx} invalid Spark hardware metadata for {scale}")
            expected_vcpu_s = round(rec["wall_clock_sec"] * rec["total_worker_vcpus"], 4)
            if abs(rec["vcpu_seconds"] - expected_vcpu_s) > 1e-3:
                raise ValueError(f"Record {idx} vcpu_seconds mismatch")
            if any(rec[k] is not None for k in ("bq_slot_cap", "bq_total_slot_ms", "bq_bytes_processed")):
                raise ValueError(f"Record {idx} Spark row must keep BQ fields null")
        else:
            if (
                rec["bq_slot_cap"] != hw["bq_slot_cap"]
                or not rec["bq_total_slot_ms"]
                or not rec["bq_bytes_processed"]
            ):
                raise ValueError(f"Record {idx} missing or mismatched BigQuery telemetry for {scale}")
            if any(rec[k] is not None for k in ("worker_machine_type", "total_worker_vcpus", "vcpu_seconds")):
                raise ValueError(f"Record {idx} BigFrames row must keep Spark fields null")

        if engine == "spark_lightning_nqe":
            if not rec["nqe_enabled"]:
                raise ValueError(f"Record {idx} spark_lightning_nqe must set nqe_enabled=True")
            if stage == "stage2b_silver_udf" and not rec["nqe_fallback_note"]:
                raise ValueError(f"Record {idx} must populate nqe_fallback_note on stage2b_silver_udf")
        elif rec["nqe_enabled"]:
            raise ValueError(f"Record {idx} nqe_enabled must be False for {engine}")

        by_scale_stage[(scale, stage)].append(rec)

    # Cross-engine row-count 100% parity & checksum within +/- 0.0001% within each (scale, stage)
    for (scale, stage), stage_rows in by_scale_stage.items():
        if len(stage_rows) != len(ENGINE_SPECS):
            raise ValueError(f"{scale}/{stage} expected {len(ENGINE_SPECS)} engine rows, got {len(stage_rows)}")
        row_counts = {r["output_row_count"] for r in stage_rows}
        if len(row_counts) != 1:
            raise ValueError(f"{scale}/{stage} row count parity failed: {row_counts}")

        ref_checksum = stage_rows[0]["checksum_net_paid"]
        for r in stage_rows:
            delta_pct = abs(r["checksum_net_paid"] - ref_checksum) / ref_checksum * 100.0
            if delta_pct > 0.0001:
                raise ValueError(
                    f"{scale}/{stage} checksum drift {delta_pct:.6f}% exceeds +/-0.0001% for {r['engine_variant']}"
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
    parser = argparse.ArgumentParser(description="Generate and validate 3-scale sample benchmark telemetry JSONL.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic PRNG seed (default: 42)")
    parser.add_argument(
        "--run-id",
        type=str,
        default=RUN_ID_PREFIX_DEFAULT,
        help="Run identifier prefix; emitted as <run-id>_<scale_preset> (default: sample_dryrun)",
    )
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

    records = generate_sample_records(seed=args.seed, run_id_prefix=args.run_id)
    validate_records(records)
    write_jsonl(records, args.output)
    reloaded = load_jsonl(args.output)
    validate_records(reloaded)
    print(
        f"GENERATED & VALIDATED {len(reloaded)} records "
        f"(scales={','.join(SCALE_ORDER)}, seed={args.seed}, prefix={args.run_id}) -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
