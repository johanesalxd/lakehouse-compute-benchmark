#!/usr/bin/env python3
"""Deterministic generator and validator for offline 3-scale, 7-variant benchmark telemetry.

Generates a realistic 75-record multi-scale dataset across ``small_1g``,
``medium_100g``, and ``large_500g`` (3 scales x [6 staged variants x 4 stages +
1 lazy end-to-end variant x 1 E2E stage = 25 rows/scale]) matching the exact
21-field schema in ``SPEC.md`` §4 (including ``sample_fraction=0.05`` on Stage 2B
for ``medium_100g`` and ``large_500g``) and writes it to
``results/sample_benchmark_runs.jsonl``.
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

STAGED_STAGE_ORDER: tuple[str, ...] = (
    "stage1_bronze_to_silver",
    "stage2a_silver_vectorized",
    "stage2b_silver_udf",
    "stage3_silver_to_gold",
)

STAGE_ORDER: tuple[str, ...] = (
    *STAGED_STAGE_ORDER,
    "stage_e2e_bronze_to_gold",
)

ENGINE_SPECS: dict[str, dict[str, Any]] = {
    "spark_standard": {
        "storage_backend": "gcs_parquet",
        "is_spark": True,
        "nqe_enabled": False,
        "stages": STAGED_STAGE_ORDER,
    },
    "spark_lightning_base": {
        "storage_backend": "gcs_parquet",
        "is_spark": True,
        "nqe_enabled": False,
        "stages": STAGED_STAGE_ORDER,
    },
    "spark_lightning_nqe": {
        "storage_backend": "gcs_parquet",
        "is_spark": True,
        "nqe_enabled": True,
        "stages": STAGED_STAGE_ORDER,
    },
    "bigframes_biglake_gcs": {
        "storage_backend": "biglake_gcs_parquet",
        "is_spark": False,
        "nqe_enabled": False,
        "stages": STAGED_STAGE_ORDER,
    },
    "bigframes_native_bq": {
        "storage_backend": "bq_native_capacitor",
        "is_spark": False,
        "nqe_enabled": False,
        "stages": STAGED_STAGE_ORDER,
    },
    "bigframes_ctas_native_bq": {
        "storage_backend": "bq_native_capacitor",
        "is_spark": False,
        "nqe_enabled": False,
        "stages": STAGED_STAGE_ORDER,
    },
    "bigframes_lazy_e2e_bq": {
        "storage_backend": "bq_native_capacitor",
        "is_spark": False,
        "nqe_enabled": False,
        "stages": ("stage_e2e_bronze_to_gold",),
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
# Calibrated proportionally to observed reference magnitudes across small_1g / medium_100g / large_500g
# (with Pass 6 Clustered CTAS and Pass 7 Lazy E2E Fusion reflecting conservative synthetic estimates).
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
            "stage_e2e_bronze_to_gold": {
                "output_row_count": 424_486,
                "base_checksum_net_paid": 5_839_057_120.80,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 16.60},
                "stage2a_silver_vectorized": {"wall_sec": 6.15},
                "stage2b_silver_udf": {"wall_sec": 7.33},
                "stage3_silver_to_gold": {"wall_sec": 17.08},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 16.96},
                "stage2a_silver_vectorized": {"wall_sec": 6.69},
                "stage2b_silver_udf": {"wall_sec": 7.56},
                "stage3_silver_to_gold": {"wall_sec": 16.89},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 14.47},
                "stage2a_silver_vectorized": {"wall_sec": 5.92},
                "stage2b_silver_udf": {
                    "wall_sec": 8.99,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 10.53},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 11.15,
                    "slot_ms": 32_732,
                    "bytes_processed": 295_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 6.90,
                    "slot_ms": 3_033,
                    "bytes_processed": 210_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 7.17,
                    "slot_ms": 35_983,
                    "bytes_processed": 381_200_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 25.52,
                    "slot_ms": 47_779,
                    "bytes_processed": 327_600_000,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 11.43,
                    "slot_ms": 36_437,
                    "bytes_processed": 280_100_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 7.07,
                    "slot_ms": 40_698,
                    "bytes_processed": 210_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 7.02,
                    "slot_ms": 2_306,
                    "bytes_processed": 210_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 26.70,
                    "slot_ms": 50_269,
                    "bytes_processed": 327_600_000,
                },
            },
            "bigframes_ctas_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 7.40,
                    "slot_ms": 17_500,
                    "bytes_processed": 280_100_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 4.80,
                    "slot_ms": 18_500,
                    "bytes_processed": 210_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 4.90,
                    "slot_ms": 1_600,
                    "bytes_processed": 195_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 15.80,
                    "slot_ms": 24_500,
                    "bytes_processed": 310_000_000,
                },
            },
            "bigframes_lazy_e2e_bq": {
                "stage_e2e_bronze_to_gold": {
                    "wall_sec": 14.50,
                    "slot_ms": 42_000,
                    "bytes_processed": 327_600_000,
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
                "output_row_count": 256_230_230,
                "base_checksum_net_paid": 469_611_312_771.88,
                "sample_fraction": 1.0,
            },
            "stage2a_silver_vectorized": {
                "output_row_count": 256_230_230,
                "base_checksum_net_paid": 469_611_312_771.88,
                "sample_fraction": 1.0,
            },
            "stage2b_silver_udf": {
                "output_row_count": 12_899_857,
                "base_checksum_net_paid": 23_721_909_662.83,
                "sample_fraction": 0.050345,
            },
            "stage3_silver_to_gold": {
                "output_row_count": 5_363,
                "base_checksum_net_paid": 468_454_107_652.46,
                "sample_fraction": 1.0,
            },
            "stage_e2e_bronze_to_gold": {
                "output_row_count": 5_363,
                "base_checksum_net_paid": 468_454_107_652.46,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 70.00},
                "stage2a_silver_vectorized": {"wall_sec": 35.85},
                "stage2b_silver_udf": {"wall_sec": 12.26},
                "stage3_silver_to_gold": {"wall_sec": 35.37},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 58.48},
                "stage2a_silver_vectorized": {"wall_sec": 26.84},
                "stage2b_silver_udf": {"wall_sec": 14.56},
                "stage3_silver_to_gold": {"wall_sec": 31.38},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 45.59},
                "stage2a_silver_vectorized": {"wall_sec": 22.41},
                "stage2b_silver_udf": {
                    "wall_sec": 9.96,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 23.11},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 82.90,
                    "slot_ms": 11_988_580,
                    "bytes_processed": 41_850_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 86.70,
                    "slot_ms": 16_085_060,
                    "bytes_processed": 29_400_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 16.30,
                    "slot_ms": 880_040,
                    "bytes_processed": 29_400_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 33.32,
                    "slot_ms": 3_270_460,
                    "bytes_processed": 46_200_000_000,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 81.75,
                    "slot_ms": 13_672_410,
                    "bytes_processed": 36_500_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 89.11,
                    "slot_ms": 16_172_250,
                    "bytes_processed": 29_400_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 16.75,
                    "slot_ms": 924_090,
                    "bytes_processed": 29_400_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 31.80,
                    "slot_ms": 3_340_060,
                    "bytes_processed": 38_200_000_000,
                },
            },
            "bigframes_ctas_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 42.50,
                    "slot_ms": 6_900_000,
                    "bytes_processed": 36_500_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 44.00,
                    "slot_ms": 7_900_000,
                    "bytes_processed": 29_400_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 10.20,
                    "slot_ms": 520_000,
                    "bytes_processed": 24_800_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 19.50,
                    "slot_ms": 1_850_000,
                    "bytes_processed": 34_500_000_000,
                },
            },
            "bigframes_lazy_e2e_bq": {
                "stage_e2e_bronze_to_gold": {
                    "wall_sec": 64.00,
                    "slot_ms": 10_800_000,
                    "bytes_processed": 38_200_000_000,
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
            "bq_slot_cap": 400,
        },
        "stage_parity": {
            "stage1_bronze_to_silver": {
                "output_row_count": 1_014_392_792,
                "base_checksum_net_paid": 1_841_729_847_639.58,
                "sample_fraction": 1.0,
            },
            "stage2a_silver_vectorized": {
                "output_row_count": 1_014_392_792,
                "base_checksum_net_paid": 1_841_729_847_639.58,
                "sample_fraction": 1.0,
            },
            "stage2b_silver_udf": {
                "output_row_count": 50_660_213,
                "base_checksum_net_paid": 92_004_035_538.14,
                "sample_fraction": 0.049941,
            },
            "stage3_silver_to_gold": {
                "output_row_count": 28_068,
                "base_checksum_net_paid": 1_837_183_563_044.75,
                "sample_fraction": 1.0,
            },
            "stage_e2e_bronze_to_gold": {
                "output_row_count": 28_068,
                "base_checksum_net_paid": 1_837_183_563_044.75,
                "sample_fraction": 1.0,
            },
        },
        "stage_profiles": {
            "spark_standard": {
                "stage1_bronze_to_silver": {"wall_sec": 178.56},
                "stage2a_silver_vectorized": {"wall_sec": 97.38},
                "stage2b_silver_udf": {"wall_sec": 31.64},
                "stage3_silver_to_gold": {"wall_sec": 106.12},
            },
            "spark_lightning_base": {
                "stage1_bronze_to_silver": {"wall_sec": 140.96},
                "stage2a_silver_vectorized": {"wall_sec": 83.17},
                "stage2b_silver_udf": {"wall_sec": 26.47},
                "stage3_silver_to_gold": {"wall_sec": 89.80},
            },
            "spark_lightning_nqe": {
                "stage1_bronze_to_silver": {"wall_sec": 112.91},
                "stage2a_silver_vectorized": {"wall_sec": 73.86},
                "stage2b_silver_udf": {
                    "wall_sec": 26.09,
                    "nqe_fallback_note": NQE_FALLBACK_NOTE_UDF,
                },
                "stage3_silver_to_gold": {"wall_sec": 69.69},
            },
            "bigframes_biglake_gcs": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 148.00,
                    "slot_ms": 46_800_000,
                    "bytes_processed": 165_000_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 156.00,
                    "slot_ms": 62_500_000,
                    "bytes_processed": 116_000_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 24.50,
                    "slot_ms": 3_400_000,
                    "bytes_processed": 116_000_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 64.00,
                    "slot_ms": 12_900_000,
                    "bytes_processed": 182_000_000_000,
                },
            },
            "bigframes_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 152.00,
                    "slot_ms": 52_800_000,
                    "bytes_processed": 144_000_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 162.00,
                    "slot_ms": 63_200_000,
                    "bytes_processed": 116_000_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 25.20,
                    "slot_ms": 3_550_000,
                    "bytes_processed": 116_000_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 62.00,
                    "slot_ms": 13_100_000,
                    "bytes_processed": 150_500_000_000,
                },
            },
            "bigframes_ctas_native_bq": {
                "stage1_bronze_to_silver": {
                    "wall_sec": 78.00,
                    "slot_ms": 26_500_000,
                    "bytes_processed": 144_000_000_000,
                },
                "stage2a_silver_vectorized": {
                    "wall_sec": 82.00,
                    "slot_ms": 30_800_000,
                    "bytes_processed": 116_000_000_000,
                },
                "stage2b_silver_udf": {
                    "wall_sec": 15.50,
                    "slot_ms": 2_050_000,
                    "bytes_processed": 98_000_000_000,
                },
                "stage3_silver_to_gold": {
                    "wall_sec": 36.00,
                    "slot_ms": 7_100_000,
                    "bytes_processed": 136_000_000_000,
                },
            },
            "bigframes_lazy_e2e_bq": {
                "stage_e2e_bronze_to_gold": {
                    "wall_sec": 118.00,
                    "slot_ms": 41_500_000,
                    "bytes_processed": 150_500_000_000,
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
    """Build deterministic 75-record 3-scale, 7-variant telemetry (small_1g, medium_100g, large_500g)."""
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

            for stage_id in spec["stages"]:
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


def _parity_group_for_stage(stage_id: str) -> str:
    if stage_id in ("stage3_silver_to_gold", "stage_e2e_bronze_to_gold"):
        return "gold_kpi_mart"
    return stage_id


def validate_records(records: list[dict[str, Any]]) -> None:
    """Validate schema, per-scale hardware/sample invariants, and 7-variant Gold/stage parity."""
    per_scale_expected = sum(len(spec["stages"]) for spec in ENGINE_SPECS.values())
    expected_count = len(SCALE_ORDER) * per_scale_expected
    if len(records) != expected_count:
        raise ValueError(f"Expected {expected_count} records across {len(SCALE_ORDER)} scales, got {len(records)}")

    seen_triples: set[tuple[str, str, str]] = set()
    parity_groups = ("stage1_bronze_to_silver", "stage2a_silver_vectorized", "stage2b_silver_udf", "gold_kpi_mart")
    by_scale_group: dict[tuple[str, str], list[dict[str, Any]]] = {
        (scale, grp): [] for scale in SCALE_ORDER for grp in parity_groups
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
        if stage not in ENGINE_SPECS[engine]["stages"]:
            raise ValueError(f"Record {idx} invalid stage_id {stage} for engine {engine}")
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

        by_scale_group[(scale, _parity_group_for_stage(stage))].append(rec)

    # Cross-engine row-count 100% parity & checksum within +/- 0.0001% within each (scale, group)
    for (scale, grp), group_rows in by_scale_group.items():
        expected_engines = 7 if grp == "gold_kpi_mart" else 6
        if len(group_rows) != expected_engines:
            raise ValueError(f"{scale}/{grp} expected {expected_engines} engine rows, got {len(group_rows)}")
        row_counts = {r["output_row_count"] for r in group_rows}
        if len(row_counts) != 1:
            raise ValueError(f"{scale}/{grp} row count parity failed: {row_counts}")

        ref_checksum = group_rows[0]["checksum_net_paid"]
        for r in group_rows:
            delta_pct = abs(r["checksum_net_paid"] - ref_checksum) / ref_checksum * 100.0
            if delta_pct > 0.0001:
                raise ValueError(
                    f"{scale}/{grp} checksum drift {delta_pct:.6f}% exceeds +/-0.0001% for {r['engine_variant']}"
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
    parser = argparse.ArgumentParser(description="Generate and validate 3-scale, 7-variant sample benchmark telemetry JSONL.")
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
