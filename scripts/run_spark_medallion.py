#!/usr/bin/env python3
"""Shared in-cluster PySpark Medallion runner for Dataproc Spark benchmarks.

Executes the identical 3-stage Medallion pipeline (Stage 1 Bronze->Silver,
Stage 2A Silver vectorized, Stage 2B Silver Python UDF, Stage 3 Silver->Gold)
purely over open Parquet files in Google Cloud Storage (zero BigQuery reads or
writes) across Standard Spark (`spark_standard`), Base Lightning Engine
(`spark_lightning_base`), and Lightning Engine + Gluten/Velox NQE
(`spark_lightning_nqe`).

Only Python stdlib and PySpark are required on Dataproc cluster nodes.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import re
import sys
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

VALID_ENGINE_VARIANTS = (
    "spark_standard",
    "spark_lightning_base",
    "spark_lightning_nqe",
)
VALID_SCALE_PRESETS = ("small_1g", "medium_100g", "large_500g")
STORAGE_BACKEND = "gcs_parquet"


def _classify_order_risk_tier(
    return_amt: float | None,
    gross_sales_price: float | None,
    return_qty: int | None,
    net_profit_after_return: float | None,
    discount_amt: float | None,
) -> str:
    """Apply the deterministic 4-tier order risk classification rule."""
    ret_amt = 0.0 if return_amt is None else float(return_amt)
    gross = 0.0 if gross_sales_price is None else float(gross_sales_price)
    ret_qty = 0 if return_qty is None else int(return_qty)
    net_prof = 0.0 if net_profit_after_return is None else float(net_profit_after_return)
    disc = 0.0 if discount_amt is None else float(discount_amt)

    if ret_amt > 0.5 * gross and ret_qty > 0:
        return "HIGH_RETURN_RISK"
    if net_prof > 50.0 and disc == 0.0:
        return "VIP_FULL_PRICE"
    if disc > 0.25 * gross:
        return "PROMO_DRIVEN"
    return "STANDARD"


def _compute_adjusted_margin_score(
    net_profit_after_return: float | None,
    return_amt: float | None,
    net_paid_after_return: float | None,
) -> float:
    """Compute adjusted margin score rounded to 4 decimal places."""
    net_prof = 0.0 if net_profit_after_return is None else float(net_profit_after_return)
    ret_amt = 0.0 if return_amt is None else float(return_amt)
    net_paid_ret = 0.0 if net_paid_after_return is None else float(net_paid_after_return)
    return round(net_prof - (0.15 * ret_amt) + (0.05 * net_paid_ret), 4)


classify_order_risk_tier_udf = F.udf(_classify_order_risk_tier, T.StringType())
compute_adjusted_margin_score_udf = F.udf(_compute_adjusted_margin_score, T.DoubleType())


def _parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in ("true", "1", "yes"):
        return True
    if normalized in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError(f"Expected 'true' or 'false', got {raw!r}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the shared Medallion Spark benchmark runner."""
    parser = argparse.ArgumentParser(
        description="Run the TPC-DS 3-stage Medallion benchmark on Spark over GCS Parquet."
    )
    parser.add_argument(
        "--engine-variant",
        required=True,
        choices=VALID_ENGINE_VARIANTS,
        help="Spark engine variant identifier.",
    )
    parser.add_argument(
        "--scale-preset",
        required=True,
        choices=VALID_SCALE_PRESETS,
        help="TPC-DS dataset scale preset.",
    )
    parser.add_argument(
        "--bench-bucket",
        required=True,
        help="GCS bucket name without gs:// prefix.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Benchmark run identifier (e.g. 20261116T080000Z_small_1g).",
    )
    parser.add_argument(
        "--worker-machine-type",
        default="n2d-standard-8",
        help="Worker GCE machine type for telemetry metadata.",
    )
    parser.add_argument(
        "--total-worker-vcpus",
        type=int,
        default=16,
        help="Total primary worker vCPUs across cluster.",
    )
    parser.add_argument(
        "--run-python-udf",
        default="true",
        choices=("true", "false", "True", "False"),
        help="Whether to run Stage 2B scalar Python UDF benchmark ('true' | 'false').",
    )
    parser.add_argument(
        "--udf-max-sample-rows",
        type=int,
        default=500000,
        help="Reference sample row cap metadata for Stage 2B.",
    )
    parser.add_argument(
        "--metrics-gcs-uri",
        required=True,
        help="Destination gs:// URI for benchmark JSON payload.",
    )
    args = parser.parse_args(argv)
    args.bench_bucket = args.bench_bucket.strip().removeprefix("gs://").strip("/")
    if not args.bench_bucket:
        parser.error("--bench-bucket must not be empty")
    if args.total_worker_vcpus <= 0:
        parser.error("--total-worker-vcpus must be > 0")
    if args.udf_max_sample_rows <= 0:
        parser.error("--udf-max-sample-rows must be > 0")
    args.run_python_udf_bool = _parse_bool(args.run_python_udf)
    return args


def _list_sql_execution_ids(spark: SparkSession) -> set[int]:
    """Return Spark SQL AppStatusStore execution IDs currently recorded on driver."""
    try:
        store = spark._jsparkSession.sharedState().statusStore()
        seq = store.executionsList()
        return {int(seq.apply(i).executionId()) for i in range(seq.size())}
    except Exception:  # noqa: BLE001
        return set()


def _trim_aqe_initial_plan(plan_text: str) -> str:
    """Strip unexecuted AQE Initial Plan tree lines when a Final Plan is present."""
    if "+- == Final Plan ==" not in plan_text or "+- == Initial Plan ==" not in plan_text:
        return plan_text
    out_lines: list[str] = []
    skip_initial = False
    in_details = False
    for line in plan_text.splitlines():
        stripped = line.strip()
        if not in_details and re.match(r"^\(\d+\)\s+", stripped):
            in_details = True
            skip_initial = False
        if not in_details:
            if stripped.startswith("+- == Initial Plan =="):
                skip_initial = True
                continue
            if skip_initial:
                if not stripped:
                    skip_initial = False
                else:
                    continue
        out_lines.append(line)
    return "\n".join(out_lines)


def capture_formatted_plan(
    df: DataFrame,
    spark: SparkSession,
    prior_exec_ids: set[int] | None = None,
) -> str:
    """Return the executed physical plan when available, falling back to df explain."""
    if prior_exec_ids is not None:
        try:
            spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(5000)
        except Exception:  # noqa: BLE001
            pass
        try:
            store = spark._jsparkSession.sharedState().statusStore()
            seq = store.executionsList()
            candidates: list[tuple[int, str]] = []
            for i in range(seq.size()):
                ex = seq.apply(i)
                eid = int(ex.executionId())
                if eid not in prior_exec_ids:
                    phys = str(ex.physicalPlanDescription() or "")
                    if phys.strip():
                        candidates.append((eid, phys))
            if candidates:
                candidates.sort(key=lambda item: item[0])
                return _trim_aqe_initial_plan(candidates[-1][1])
        except Exception:  # noqa: BLE001
            pass

    try:
        explain_mode = spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(
            "formatted"
        )
        return str(df._jdf.queryExecution().explainString(explain_mode))
    except Exception as exc:  # noqa: BLE001
        return f"Plan capture fallback: {exc}\n{df._jdf.queryExecution().simpleString()}"


def detect_nqe_fallback_note(
    engine_variant: str,
    stage_id: str,
    plan_text: str,
    lightning_runtime: str,
) -> str:
    """Detect NQE JVM/Python fallback operators in formatted physical plans."""
    if engine_variant != "spark_lightning_nqe":
        return ""

    notes: list[str] = []
    if lightning_runtime != "native":
        notes.append(f"expected runtime=native, observed={lightning_runtime}")

    python_markers = [
        m
        for m in ("BatchEvalPythonExec", "ArrowEvalPythonExec")
        if m in plan_text
    ]
    transition_markers = [
        m
        for m in ("VeloxColumnarToRow", "RowToVeloxColumnar")
        if m in plan_text
    ]

    if stage_id == "stage2b_silver_udf":
        found = python_markers + transition_markers
        if found:
            notes.append(
                "JVM/Python UDF fallback detected: " + ", ".join(found)
            )
        else:
            notes.append(
                "Scalar Python UDF executes outside Gluten/Velox native C++ pipeline"
            )
    elif python_markers:
        notes.append(
            f"Unexpected Python fallback in {stage_id}: {', '.join(python_markers)}"
        )

    return "; ".join(notes)


def write_json_to_hadoop_uri(
    spark: SparkSession, uri: str, payload: dict[str, Any]
) -> None:
    """Write UTF-8 JSON to gs:// or file:// using Hadoop FileSystem on the driver."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        hadoop_path.toUri(), hadoop_conf
    )
    parent = hadoop_path.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    out_stream = fs.create(hadoop_path, True)
    try:
        encoded = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
        out_stream.write(encoded)
    finally:
        out_stream.close()


def _build_channel_sales_df(
    channel_name: str,
    sales_df: DataFrame,
    returns_df: DataFrame,
    *,
    sold_date_sk_col: str,
    item_sk_col: str,
    customer_sk_col: str,
    cdemo_sk_col: str,
    order_col: str,
    qty_col: str,
    ext_sales_col: str,
    ext_disc_col: str,
    net_paid_col: str,
    net_profit_col: str,
    ret_item_col: str,
    ret_order_col: str,
    ret_qty_col: str,
    ret_amt_col: str,
    ret_loss_col: str,
) -> DataFrame:
    """Clean sales, pre-aggregate returns to prevent fan-out, and left-join."""
    clean_sales = sales_df.filter(
        F.col(sold_date_sk_col).isNotNull()
        & F.col(item_sk_col).isNotNull()
        & F.col(customer_sk_col).isNotNull()
        & (F.col(qty_col) > F.lit(0))
    )

    returns_agg = (
        returns_df.filter(
            F.col(ret_item_col).isNotNull() & F.col(ret_order_col).isNotNull()
        )
        .groupBy(
            F.col(ret_item_col).cast(T.LongType()).alias("ret_item_sk"),
            F.col(ret_order_col).cast(T.LongType()).alias("ret_order_number"),
        )
        .agg(
            F.sum(F.coalesce(F.col(ret_qty_col).cast(T.LongType()), F.lit(0)))
            .cast(T.LongType())
            .alias("agg_return_qty"),
            F.sum(F.coalesce(F.col(ret_amt_col).cast(T.DoubleType()), F.lit(0.0)))
            .cast(T.DoubleType())
            .alias("agg_return_amt"),
            F.sum(F.coalesce(F.col(ret_loss_col).cast(T.DoubleType()), F.lit(0.0)))
            .cast(T.DoubleType())
            .alias("agg_return_net_loss"),
        )
    )

    net_paid_expr = F.coalesce(F.col(net_paid_col).cast(T.DoubleType()), F.lit(0.0))
    net_profit_expr = F.coalesce(F.col(net_profit_col).cast(T.DoubleType()), F.lit(0.0))
    ret_amt_expr = F.coalesce(F.col("agg_return_amt"), F.lit(0.0)).cast(T.DoubleType())
    ret_loss_expr = F.coalesce(F.col("agg_return_net_loss"), F.lit(0.0)).cast(T.DoubleType())

    return clean_sales.join(
        returns_agg,
        (clean_sales[item_sk_col].cast(T.LongType()) == returns_agg["ret_item_sk"])
        & (clean_sales[order_col].cast(T.LongType()) == returns_agg["ret_order_number"]),
        how="left",
    ).select(
        F.lit(channel_name).cast(T.StringType()).alias("channel"),
        F.col(sold_date_sk_col).cast(T.LongType()).alias("sold_date_sk"),
        F.col(item_sk_col).cast(T.LongType()).alias("item_sk"),
        F.col(customer_sk_col).cast(T.LongType()).alias("customer_sk"),
        F.col(cdemo_sk_col).cast(T.LongType()).alias("cdemo_sk"),
        F.col(order_col).cast(T.LongType()).alias("order_number"),
        F.col(qty_col).cast(T.LongType()).alias("quantity"),
        F.coalesce(F.col(ext_sales_col).cast(T.DoubleType()), F.lit(0.0)).alias(
            "gross_sales_price"
        ),
        F.coalesce(F.col(ext_disc_col).cast(T.DoubleType()), F.lit(0.0)).alias(
            "discount_amt"
        ),
        net_paid_expr.alias("net_paid"),
        net_profit_expr.alias("net_profit"),
        F.coalesce(F.col("agg_return_qty"), F.lit(0))
        .cast(T.LongType())
        .alias("return_qty"),
        ret_amt_expr.alias("return_amt"),
        (net_paid_expr - ret_amt_expr)
        .cast(T.DoubleType())
        .alias("net_paid_after_return"),
        (net_profit_expr - ret_loss_expr)
        .cast(T.DoubleType())
        .alias("net_profit_after_return"),
    )


def _measure_output_checksum(
    spark: SparkSession, out_path: str, checksum_col: str
) -> tuple[int, float]:
    """Read back materialized Parquet outside the timer to compute exact count & checksum."""
    row = (
        spark.read.parquet(out_path)
        .agg(
            F.count(F.lit(1)).alias("cnt"),
            F.round(F.sum(F.col(checksum_col)), 2).alias("chk"),
        )
        .collect()[0]
    )
    row_count = int(row["cnt"])
    checksum = float(row["chk"]) if row["chk"] is not None else 0.0
    return row_count, checksum


def _make_stage_record(
    *,
    run_id: str,
    scale_preset: str,
    engine_variant: str,
    stage_id: str,
    start_epoch_sec: float,
    end_epoch_sec: float,
    output_row_count: int,
    checksum_net_paid: float,
    worker_machine_type: str,
    total_worker_vcpus: int,
    nqe_fallback_note: str,
    sample_fraction: float,
) -> dict[str, Any]:
    wall_clock_sec = round(end_epoch_sec - start_epoch_sec, 4)
    return {
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scale_preset": scale_preset,
        "engine_variant": engine_variant,
        "storage_backend": STORAGE_BACKEND,
        "stage_id": stage_id,
        "start_epoch_sec": round(start_epoch_sec, 6),
        "end_epoch_sec": round(end_epoch_sec, 6),
        "wall_clock_sec": wall_clock_sec,
        "output_row_count": output_row_count,
        "checksum_net_paid": checksum_net_paid,
        "worker_machine_type": worker_machine_type,
        "total_worker_vcpus": total_worker_vcpus,
        "vcpu_seconds": round(wall_clock_sec * total_worker_vcpus, 4),
        "bq_slot_cap": None,
        "bq_total_slot_ms": None,
        "bq_bytes_processed": None,
        "nqe_enabled": engine_variant == "spark_lightning_nqe",
        "nqe_fallback_note": nqe_fallback_note,
        "est_list_cost_usd": None,
        "sample_fraction": sample_fraction,
    }


def _execute_timed_stage(
    *,
    spark: SparkSession,
    stage_id: str,
    df_builder: Any,
    out_path: str,
    checksum_col: str,
    run_id: str,
    scale_preset: str,
    engine_variant: str,
    worker_machine_type: str,
    total_worker_vcpus: int,
    lightning_runtime: str,
    sample_fraction: float = 1.0,
) -> tuple[dict[str, Any], str]:
    """Execute one timed Medallion stage, capture plan, and validate output Parquet."""
    prior_exec_ids = _list_sql_execution_ids(spark)
    start_epoch_sec = time.time()
    df = df_builder()
    df.write.mode("overwrite").parquet(out_path)
    end_epoch_sec = time.time()

    plan_text = capture_formatted_plan(df, spark, prior_exec_ids=prior_exec_ids)
    output_row_count, checksum_net_paid = _measure_output_checksum(
        spark, out_path, checksum_col
    )
    fallback_note = detect_nqe_fallback_note(
        engine_variant, stage_id, plan_text, lightning_runtime
    )
    record = _make_stage_record(
        run_id=run_id,
        scale_preset=scale_preset,
        engine_variant=engine_variant,
        stage_id=stage_id,
        start_epoch_sec=start_epoch_sec,
        end_epoch_sec=end_epoch_sec,
        output_row_count=output_row_count,
        checksum_net_paid=checksum_net_paid,
        worker_machine_type=worker_machine_type,
        total_worker_vcpus=total_worker_vcpus,
        nqe_fallback_note=fallback_note,
        sample_fraction=sample_fraction,
    )
    return record, plan_text


def run_pipeline(
    *,
    spark: SparkSession,
    engine_variant: str,
    scale_preset: str,
    bronze_base_uri: str,
    silver_base_uri: str,
    gold_base_uri: str,
    run_id: str,
    worker_machine_type: str,
    total_worker_vcpus: int,
    run_python_udf: bool,
    udf_max_sample_rows: int,
) -> dict[str, Any]:
    """Execute all Medallion stages and return telemetry records + Catalyst plans."""
    lightning_runtime = spark.conf.get(
        "spark.dataproc.lightningEngine.runtime", "unset"
    )
    # Warm up executors before starting Stage 1 timer so YARN startup is excluded.
    _ = spark.range(1).count()

    bronze_base = bronze_base_uri.rstrip("/")
    silver_base = silver_base_uri.rstrip("/")
    gold_base = gold_base_uri.rstrip("/")

    stage1_out = f"{silver_base}/silver_omnichannel_sales/"
    stage2a_out = f"{silver_base}/silver_enriched_sales/"
    stage2b_out = f"{silver_base}/silver_udf_benchmark_out/"
    stage3_out = f"{gold_base}/gold_omnichannel_kpi_mart/"

    records: list[dict[str, Any]] = []
    plans: dict[str, str] = {}

    # Stage 1: Bronze -> Silver (Omnichannel Sales & Returns Reconciliation)
    def build_stage1() -> DataFrame:
        store_sales = spark.read.parquet(f"{bronze_base}/store_sales/")
        store_returns = spark.read.parquet(f"{bronze_base}/store_returns/")
        web_sales = spark.read.parquet(f"{bronze_base}/web_sales/")
        web_returns = spark.read.parquet(f"{bronze_base}/web_returns/")

        store_df = _build_channel_sales_df(
            "STORE",
            store_sales,
            store_returns,
            sold_date_sk_col="ss_sold_date_sk",
            item_sk_col="ss_item_sk",
            customer_sk_col="ss_customer_sk",
            cdemo_sk_col="ss_cdemo_sk",
            order_col="ss_ticket_number",
            qty_col="ss_quantity",
            ext_sales_col="ss_ext_sales_price",
            ext_disc_col="ss_ext_discount_amt",
            net_paid_col="ss_net_paid",
            net_profit_col="ss_net_profit",
            ret_item_col="sr_item_sk",
            ret_order_col="sr_ticket_number",
            ret_qty_col="sr_return_quantity",
            ret_amt_col="sr_return_amt",
            ret_loss_col="sr_net_loss",
        )
        web_df = _build_channel_sales_df(
            "WEB",
            web_sales,
            web_returns,
            sold_date_sk_col="ws_sold_date_sk",
            item_sk_col="ws_item_sk",
            customer_sk_col="ws_bill_customer_sk",
            cdemo_sk_col="ws_bill_cdemo_sk",
            order_col="ws_order_number",
            qty_col="ws_quantity",
            ext_sales_col="ws_ext_sales_price",
            ext_disc_col="ws_ext_discount_amt",
            net_paid_col="ws_net_paid",
            net_profit_col="ws_net_profit",
            ret_item_col="wr_item_sk",
            ret_order_col="wr_order_number",
            ret_qty_col="wr_return_quantity",
            ret_amt_col="wr_return_amt",
            ret_loss_col="wr_net_loss",
        )
        return store_df.unionByName(web_df)

    r1, p1 = _execute_timed_stage(
        spark=spark,
        stage_id="stage1_bronze_to_silver",
        df_builder=build_stage1,
        out_path=stage1_out,
        checksum_col="net_paid_after_return",
        run_id=run_id,
        scale_preset=scale_preset,
        engine_variant=engine_variant,
        worker_machine_type=worker_machine_type,
        total_worker_vcpus=total_worker_vcpus,
        lightning_runtime=lightning_runtime,
    )
    records.append(r1)
    plans["stage1_bronze_to_silver"] = p1

    # Stage 2A: Silver Vectorized Tiering & Margin Scoring
    def build_stage2a() -> DataFrame:
        silver_df = spark.read.parquet(stage1_out)
        return silver_df.withColumn(
            "order_risk_tier",
            F.when(
                (F.col("return_amt") > (F.lit(0.5) * F.col("gross_sales_price")))
                & (F.col("return_qty") > F.lit(0)),
                F.lit("HIGH_RETURN_RISK"),
            )
            .when(
                (F.col("net_profit_after_return") > F.lit(50.0))
                & (F.col("discount_amt") == F.lit(0.0)),
                F.lit("VIP_FULL_PRICE"),
            )
            .when(
                F.col("discount_amt") > (F.lit(0.25) * F.col("gross_sales_price")),
                F.lit("PROMO_DRIVEN"),
            )
            .otherwise(F.lit("STANDARD")),
        ).withColumn(
            "adjusted_margin_score",
            F.round(
                F.col("net_profit_after_return")
                - (F.lit(0.15) * F.col("return_amt"))
                + (F.lit(0.05) * F.col("net_paid_after_return")),
                4,
            ).cast(T.DoubleType()),
        )

    r2a, p2a = _execute_timed_stage(
        spark=spark,
        stage_id="stage2a_silver_vectorized",
        df_builder=build_stage2a,
        out_path=stage2a_out,
        checksum_col="net_paid_after_return",
        run_id=run_id,
        scale_preset=scale_preset,
        engine_variant=engine_variant,
        worker_machine_type=worker_machine_type,
        total_worker_vcpus=total_worker_vcpus,
        lightning_runtime=lightning_runtime,
    )
    records.append(r2a)
    plans["stage2a_silver_vectorized"] = p2a

    # Stage 2B: Silver Scalar Python UDF Contrast
    if run_python_udf:
        sample_fraction = 1.0 if scale_preset == "small_1g" else 0.05

        def build_stage2b() -> DataFrame:
            silver_df = spark.read.parquet(stage1_out)
            if scale_preset != "small_1g":
                silver_df = silver_df.filter(
                    F.pmod(F.abs(F.col("order_number")), F.lit(20)) == F.lit(0)
                )
            return silver_df.withColumn(
                "order_risk_tier",
                classify_order_risk_tier_udf(
                    F.col("return_amt"),
                    F.col("gross_sales_price"),
                    F.col("return_qty"),
                    F.col("net_profit_after_return"),
                    F.col("discount_amt"),
                ),
            ).withColumn(
                "adjusted_margin_score",
                compute_adjusted_margin_score_udf(
                    F.col("net_profit_after_return"),
                    F.col("return_amt"),
                    F.col("net_paid_after_return"),
                ),
            )

        r2b, p2b = _execute_timed_stage(
            spark=spark,
            stage_id="stage2b_silver_udf",
            df_builder=build_stage2b,
            out_path=stage2b_out,
            checksum_col="net_paid_after_return",
            run_id=run_id,
            scale_preset=scale_preset,
            engine_variant=engine_variant,
            worker_machine_type=worker_machine_type,
            total_worker_vcpus=total_worker_vcpus,
            lightning_runtime=lightning_runtime,
            sample_fraction=sample_fraction,
        )
        silver_rows = int(r1["output_row_count"])
        sampled_rows = int(r2b["output_row_count"])
        if silver_rows > 0 and sampled_rows > 0:
            r2b["sample_fraction"] = min(1.0, round(sampled_rows / silver_rows, 6))
        records.append(r2b)
        plans["stage2b_silver_udf"] = p2b
    else:
        plans["stage2b_silver_udf"] = "SKIPPED (--run-python-udf=false)"

    # Stage 3: Silver -> Gold (5-Way Star Join + Rolling 7d/30d Windows)
    def build_stage3() -> DataFrame:
        s = spark.read.parquet(stage2a_out).alias("s")
        c = spark.read.parquet(f"{bronze_base}/customer/").select(
            "c_customer_sk", "c_current_cdemo_sk"
        ).alias("c")
        cd = spark.read.parquet(f"{bronze_base}/customer_demographics/").select(
            "cd_demo_sk", "cd_credit_rating"
        ).alias("cd")
        i = spark.read.parquet(f"{bronze_base}/item/").select(
            "i_item_sk", "i_category"
        ).alias("i")
        d = spark.read.parquet(f"{bronze_base}/date_dim/").select(
            "d_date_sk", "d_date", "d_year", "d_moy"
        ).alias("d")

        joined = (
            s.join(c, F.col("s.customer_sk") == F.col("c.c_customer_sk"), how="inner")
            .join(
                cd,
                F.coalesce(F.col("s.cdemo_sk"), F.col("c.c_current_cdemo_sk"))
                == F.col("cd.cd_demo_sk"),
                how="inner",
            )
            .join(i, F.col("s.item_sk") == F.col("i.i_item_sk"), how="inner")
            .join(d, F.col("s.sold_date_sk") == F.col("d.d_date_sk"), how="inner")
            .filter(
                F.col("i.i_category").isNotNull()
                & F.col("cd.cd_credit_rating").isNotNull()
                & F.col("d.d_date").isNotNull()
            )
        )

        daily_agg = joined.groupBy(
            F.col("d.d_date_sk").cast(T.LongType()).alias("d_date_sk"),
            F.col("d.d_date").alias("d_date"),
            F.col("d.d_year").cast(T.LongType()).alias("d_year"),
            F.col("d.d_moy").cast(T.LongType()).alias("d_moy"),
            F.col("s.channel").alias("channel"),
            F.col("i.i_category").alias("i_category"),
            F.col("cd.cd_credit_rating").alias("cd_credit_rating"),
            F.col("s.order_risk_tier").alias("order_risk_tier"),
        ).agg(
            F.count(F.lit(1)).cast(T.LongType()).alias("order_lines"),
            F.countDistinct(F.col("s.customer_sk"))
            .cast(T.LongType())
            .alias("unique_customers"),
            F.sum(F.col("s.gross_sales_price"))
            .cast(T.DoubleType())
            .alias("daily_gross_sales"),
            F.sum(F.col("s.return_amt"))
            .cast(T.DoubleType())
            .alias("daily_return_amt"),
            F.sum(F.col("s.net_paid_after_return"))
            .cast(T.DoubleType())
            .alias("daily_net_paid"),
            F.sum(F.col("s.net_profit_after_return"))
            .cast(T.DoubleType())
            .alias("daily_net_profit"),
            F.avg(F.col("s.adjusted_margin_score"))
            .cast(T.DoubleType())
            .alias("avg_margin_score"),
        )

        w_base = Window.partitionBy(
            "channel", "i_category", "cd_credit_rating", "order_risk_tier"
        ).orderBy(F.col("d_date_sk").asc())
        w_7d = w_base.rowsBetween(-6, 0)
        w_30d = w_base.rowsBetween(-29, 0)
        w_daily_channel = Window.partitionBy("d_date_sk", "channel")

        rolling_30d_ret = F.sum(F.col("daily_return_amt")).over(w_30d)
        rolling_30d_gross = F.sum(F.col("daily_gross_sales")).over(w_30d)
        daily_channel_paid = F.sum(F.col("daily_net_paid")).over(w_daily_channel)

        return (
            daily_agg.withColumn(
                "rolling_7d_net_paid",
                F.sum(F.col("daily_net_paid")).over(w_7d).cast(T.DoubleType()),
            )
            .withColumn(
                "rolling_30d_net_paid",
                F.sum(F.col("daily_net_paid")).over(w_30d).cast(T.DoubleType()),
            )
            .withColumn(
                "rolling_30d_return_rate",
                F.when(
                    rolling_30d_gross.isNotNull()
                    & (rolling_30d_gross != F.lit(0.0)),
                    rolling_30d_ret / rolling_30d_gross,
                )
                .otherwise(F.lit(None).cast(T.DoubleType()))
                .cast(T.DoubleType()),
            )
            .withColumn(
                "category_daily_share",
                F.when(
                    daily_channel_paid.isNotNull()
                    & (daily_channel_paid != F.lit(0.0)),
                    F.col("daily_net_paid") / daily_channel_paid,
                )
                .otherwise(F.lit(None).cast(T.DoubleType()))
                .cast(T.DoubleType()),
            )
        )

    r3, p3 = _execute_timed_stage(
        spark=spark,
        stage_id="stage3_silver_to_gold",
        df_builder=build_stage3,
        out_path=stage3_out,
        checksum_col="daily_net_paid",
        run_id=run_id,
        scale_preset=scale_preset,
        engine_variant=engine_variant,
        worker_machine_type=worker_machine_type,
        total_worker_vcpus=total_worker_vcpus,
        lightning_runtime=lightning_runtime,
    )
    records.append(r3)
    plans["stage3_silver_to_gold"] = p3

    return {
        "run_id": run_id,
        "engine_variant": engine_variant,
        "scale_preset": scale_preset,
        "lightning_runtime_conf": lightning_runtime,
        "udf_max_sample_rows": udf_max_sample_rows,
        "records": records,
        "plans": plans,
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Dataproc PySpark submission."""
    args = parse_args(argv)
    spark = (
        SparkSession.builder.appName(
            f"lakehouse-bench-{args.engine_variant}-{args.scale_preset}-{args.run_id}"
        ).getOrCreate()
    )
    try:
        bronze_base = f"gs://{args.bench_bucket}/bronze/{args.scale_preset}"
        silver_base = (
            f"gs://{args.bench_bucket}/silver/{args.scale_preset}/{args.engine_variant}"
        )
        gold_base = (
            f"gs://{args.bench_bucket}/gold/{args.scale_preset}/{args.engine_variant}"
        )

        payload = run_pipeline(
            spark=spark,
            engine_variant=args.engine_variant,
            scale_preset=args.scale_preset,
            bronze_base_uri=bronze_base,
            silver_base_uri=silver_base,
            gold_base_uri=gold_base,
            run_id=args.run_id,
            worker_machine_type=args.worker_machine_type,
            total_worker_vcpus=args.total_worker_vcpus,
            run_python_udf=args.run_python_udf_bool,
            udf_max_sample_rows=args.udf_max_sample_rows,
        )

        write_json_to_hadoop_uri(spark, args.metrics_gcs_uri, payload)
        print("BENCHMARK_METRICS_JSON_BEGIN")
        print(json.dumps(payload, indent=2))
        print("BENCHMARK_METRICS_JSON_END")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
