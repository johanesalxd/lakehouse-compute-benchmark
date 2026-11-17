#!/usr/bin/env python3
"""Cross-region TPC-DS Bronze ingestion via BigQuery Storage Read API.

Supports regional GCP projects (e.g. ``asia-southeast1`` or ``us-central1``)
locked by ``constraints/gcp.resourceLocations`` where SQL ``CTAS`` from the
``US`` multi-region ``bigquery-public-data`` datasets is disallowed.

Design guarantees:
- Fail fast (``ValueError`` / exit ``1``) if ``BQ_LOCATION != "US"`` and
  ``SCALE_PRESET != "small_1g"``. (``medium_100g`` and ``large_500g`` slice
  ``tpc_ds_10t`` intra-``US`` and require ``BQ_LOCATION=US``.)
- Streams the 8 benchmark TPC-DS tables from ``bigquery-public-data.tpc_ds_1g``
  through BigQuery Storage Read API (Arrow IPC -> Snappy Parquet), billing read
  sessions to ``projects/{PROJECT_ID}`` without creating any ``US`` datasets
  or ``US`` buckets in ``PROJECT_ID``.
- Preserves 100% table names, schemas, ``CLUSTER BY`` columns, BigLake external
  tables, GCS prefix convention (default ``bronze/{SCALE_PRESET}/<table>/``),
  and ``small_1g`` row counts (``6,070,914`` total rows across the 8 tables).
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.metrics_helper import VALID_SCALE_PRESETS, resolve_bench_config

TPCDS_TABLES: list[str] = [
    "store_sales",
    "store_returns",
    "web_sales",
    "web_returns",
    "customer",
    "customer_demographics",
    "item",
    "date_dim",
]

CLUSTER_KEYS: dict[str, str] = {
    "store_sales": "ss_sold_date_sk",
    "store_returns": "sr_returned_date_sk",
    "web_sales": "ws_sold_date_sk",
    "web_returns": "wr_returned_date_sk",
    "customer": "c_customer_sk",
    "customer_demographics": "cd_demo_sk",
    "item": "i_item_sk",
    "date_dim": "d_date_sk",
}

SMALL_1G_SOURCE_PROJECT = "bigquery-public-data"
SMALL_1G_SOURCE_DATASET = "tpc_ds_1g"
SMALL_1G_EXPECTED_TOTAL_ROWS = 6_070_914


def validate_cross_region_request(bq_location: str, scale_preset: str) -> None:
    """Validate that regional cross-region streaming is only used with ``small_1g``.

    Raises:
        ValueError: If ``scale_preset`` is unknown or if ``bq_location`` is
            non-``US`` with ``medium_100g`` / ``large_500g``.
    """
    norm_loc = str(bq_location or "").strip()
    norm_scale = str(scale_preset or "").strip()

    if not norm_loc:
        raise ValueError("bq_location must be non-empty")
    if norm_scale not in VALID_SCALE_PRESETS:
        raise ValueError(
            f"Invalid SCALE_PRESET={norm_scale!r}; expected one of {sorted(VALID_SCALE_PRESETS)}"
        )

    if norm_loc.upper() != "US" and norm_scale != "small_1g":
        raise ValueError(
            f"Cross-region Storage-Read ingestion for BQ_LOCATION={norm_loc!r} (!= 'US') "
            f"only supports SCALE_PRESET='small_1g' (~250 MiB compressed from "
            f"bigquery-public-data.{SMALL_1G_SOURCE_DATASET}). "
            f"SCALE_PRESET={norm_scale!r} slices bigquery-public-data.tpc_ds_10t intra-US "
            f"and cannot be streamed cleanly across regions; set BQ_LOCATION=US for 100g/500g "
            f"or SCALE_PRESET=small_1g for regional runs."
        )


def _clear_gcs_prefix(gcs_client: Any, bucket_name: str, prefix: str) -> int:
    """Delete stale objects under ``gs://{bucket_name}/{prefix}`` before upload."""
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix))
    deleted = 0
    for idx in range(0, len(blobs), 500):
        chunk = blobs[idx : idx + 500]
        if chunk:
            bucket.delete_blobs(chunk)
            deleted += len(chunk)
    return deleted


def stream_table_to_parquet_shards(
    *,
    read_client: Any,
    project_id: str,
    table_name: str,
    output_dir: Path,
    source_project: str = SMALL_1G_SOURCE_PROJECT,
    source_dataset: str = SMALL_1G_SOURCE_DATASET,
    max_stream_count: int = 4,
    rows_per_shard: int = 1_000_000,
) -> tuple[list[Path], int]:
    """Stream one public TPC-DS table via BigQuery Storage Read API into Snappy Parquet shards."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import bigquery_storage_v1

    output_dir.mkdir(parents=True, exist_ok=True)
    parent = f"projects/{project_id}"
    table_path = f"projects/{source_project}/datasets/{source_dataset}/tables/{table_name}"

    requested_session = bigquery_storage_v1.types.ReadSession(
        table=table_path,
        data_format=bigquery_storage_v1.types.DataFormat.ARROW,
    )
    session = read_client.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=max_stream_count,
    )

    arrow_schema = pa.ipc.read_schema(pa.py_buffer(session.arrow_schema.serialized_schema))

    shard_paths: list[Path] = []
    total_rows = 0
    shard_idx = 0
    writer: pq.ParquetWriter | None = None
    rows_in_shard = 0

    try:
        for stream in session.streams:
            for response in read_client.read_rows(stream.name):
                batch_bytes = response.arrow_record_batch.serialized_record_batch
                if not batch_bytes:
                    continue
                record_batch = pa.ipc.read_record_batch(pa.py_buffer(batch_bytes), arrow_schema)
                if record_batch.num_rows == 0:
                    continue

                if writer is None:
                    shard_path = output_dir / f"part-{shard_idx:05d}.snappy.parquet"
                    writer = pq.ParquetWriter(shard_path, arrow_schema, compression="snappy")
                    shard_paths.append(shard_path)

                table_chunk = pa.Table.from_batches([record_batch], schema=arrow_schema)
                writer.write_table(table_chunk)
                total_rows += record_batch.num_rows
                rows_in_shard += record_batch.num_rows

                if rows_per_shard > 0 and rows_in_shard >= rows_per_shard:
                    writer.close()
                    writer = None
                    rows_in_shard = 0
                    shard_idx += 1

        if not shard_paths:
            empty_path = output_dir / "part-00000.snappy.parquet"
            writer = pq.ParquetWriter(empty_path, arrow_schema, compression="snappy")
            shard_paths.append(empty_path)
    finally:
        if writer is not None:
            writer.close()

    return shard_paths, total_rows


def stage_table_cross_region(
    *,
    table_name: str,
    project_id: str,
    bq_location: str,
    bench_bucket: str,
    ds_bronze_native: str,
    ds_bronze_biglake: str,
    biglake_connection_id: str,
    gcs_prefix_base: str,
    ingest_mode: str = "both",
    bq_client: Any = None,
    gcs_client: Any = None,
    read_client: Any = None,
    max_stream_count: int = 4,
    rows_per_shard: int = 1_000_000,
) -> dict[str, object]:
    """Stream one TPC-DS table cross-region, load regional BQ Native, and mount BigLake."""
    from google.cloud import bigquery, bigquery_storage_v1, storage

    if table_name not in CLUSTER_KEYS:
        raise ValueError(f"Unknown TPC-DS benchmark table: {table_name!r}")
    if ingest_mode not in {"both", "native_only"}:
        raise ValueError(f"Invalid ingest_mode={ingest_mode!r}; expected 'both' or 'native_only'")

    bq = bq_client or bigquery.Client(project=project_id, location=bq_location)
    bq_read = read_client or bigquery_storage_v1.BigQueryReadClient()
    gcs = gcs_client or (storage.Client(project=project_id) if ingest_mode == "both" else None)

    cluster_col = CLUSTER_KEYS[table_name]
    dest_native_id = f"{project_id}.{ds_bronze_native}.{table_name}"
    table_prefix = f"{gcs_prefix_base.strip('/')}/{table_name}/"

    with tempfile.TemporaryDirectory(prefix=f"tpcds_{table_name}_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        t_stream0 = time.perf_counter()
        local_shards, streamed_rows = stream_table_to_parquet_shards(
            read_client=bq_read,
            project_id=project_id,
            table_name=table_name,
            output_dir=tmp_dir,
            max_stream_count=max_stream_count,
            rows_per_shard=rows_per_shard,
        )

        deleted_blobs = 0
        gcs_bytes = 0
        uploaded_uris: list[str] = []

        if ingest_mode == "both":
            assert gcs is not None
            deleted_blobs = _clear_gcs_prefix(gcs, bench_bucket, table_prefix)
            bucket = gcs.bucket(bench_bucket)
            for shard_path in local_shards:
                blob_name = f"{table_prefix}{shard_path.name}"
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(str(shard_path))
                gcs_bytes += shard_path.stat().st_size
                uploaded_uris.append(f"gs://{bench_bucket}/{blob_name}")
        stream_upload_sec = round(time.perf_counter() - t_stream0, 2)

        t_native0 = time.perf_counter()
        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            clustering_fields=[cluster_col],
        )
        if ingest_mode == "both":
            source_uris = uploaded_uris or [f"gs://{bench_bucket}/{table_prefix}*.parquet"]
            load_job = bq.load_table_from_uri(
                source_uris,
                dest_native_id,
                job_config=load_config,
                location=bq_location,
            )
            load_job.result()
        else:
            for idx, shard_path in enumerate(local_shards):
                shard_disposition = (
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if idx == 0
                    else bigquery.WriteDisposition.WRITE_APPEND
                )
                shard_cfg = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=shard_disposition,
                    clustering_fields=[cluster_col],
                )
                with shard_path.open("rb") as fh:
                    load_job = bq.load_table_from_file(
                        fh,
                        dest_native_id,
                        job_config=shard_cfg,
                        location=bq_location,
                    )
                    load_job.result()
        native_load_sec = round(time.perf_counter() - t_native0, 2)

    biglake_sec = 0.0
    if ingest_mode == "both":
        t_bl0 = time.perf_counter()
        connection_ref = f"{project_id}.{bq_location.lower()}.{biglake_connection_id}"
        export_uri = f"gs://{bench_bucket}/{table_prefix}*.parquet"
        biglake_sql = (
            f"CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{ds_bronze_biglake}.{table_name}`\n"
            f"WITH CONNECTION `{connection_ref}`\n"
            f"OPTIONS(\n"
            f"  format='PARQUET',\n"
            f"  uris=['{export_uri}']\n"
            f")"
        )
        biglake_job = bq.query(biglake_sql, location=bq_location)
        biglake_job.result()
        biglake_sec = round(time.perf_counter() - t_bl0, 2)

    return {
        "table_name": table_name,
        "native_copy_sec": native_load_sec,
        "deleted_stale_blobs": deleted_blobs,
        "export_parquet_sec": stream_upload_sec,
        "biglake_mount_sec": biglake_sec,
        "parquet_shards": len(local_shards) if ingest_mode == "both" else 0,
        "gcs_parquet_bytes": gcs_bytes if ingest_mode == "both" else 0,
        "streamed_rows": streamed_rows,
    }


def stage_tpcds_cross_region(
    *,
    project_id: str,
    bq_location: str,
    bench_bucket: str,
    scale_preset: str = "small_1g",
    ds_bronze_native: str = "bench_bronze_native",
    ds_bronze_biglake: str = "bench_bronze_biglake",
    biglake_connection_id: str | None = None,
    gcs_prefix: str | None = None,
    ingest_mode: str = "both",
    tables: list[str] | None = None,
    bq_client: Any = None,
    gcs_client: Any = None,
    read_client: Any = None,
    max_stream_count: int = 4,
    rows_per_shard: int = 1_000_000,
) -> list[dict[str, object]]:
    """Fail-fast validated cross-region staging across all 8 Bronze TPC-DS tables."""
    validate_cross_region_request(bq_location=bq_location, scale_preset=scale_preset)

    if not project_id or project_id == "your-gcp-project-id":
        raise ValueError("PROJECT_ID must be set to a real GCP project ID.")
    if ingest_mode == "both" and not bench_bucket:
        raise ValueError("BENCH_BUCKET must be non-empty when ingest_mode='both'.")

    resolved_conn = (
        biglake_connection_id.strip()
        if biglake_connection_id and biglake_connection_id.strip()
        else f"bench-biglake-{bq_location.strip().lower()}"
    )
    resolved_prefix = (
        gcs_prefix.strip("/")
        if gcs_prefix and gcs_prefix.strip("/")
        else f"bronze/{scale_preset}"
    )
    selected_tables = tables or list(TPCDS_TABLES)

    results: list[dict[str, object]] = []
    for tbl in selected_tables:
        print(f"Streaming {tbl} cross-region (tpc_ds_1g -> {bq_location}, mode={ingest_mode})...")
        stats = stage_table_cross_region(
            table_name=tbl,
            project_id=project_id,
            bq_location=bq_location,
            bench_bucket=bench_bucket,
            ds_bronze_native=ds_bronze_native,
            ds_bronze_biglake=ds_bronze_biglake,
            biglake_connection_id=resolved_conn,
            gcs_prefix_base=resolved_prefix,
            ingest_mode=ingest_mode,
            bq_client=bq_client,
            gcs_client=gcs_client,
            read_client=read_client,
            max_stream_count=max_stream_count,
            rows_per_shard=rows_per_shard,
        )
        results.append(stats)
        print(
            f"  -> rows={stats['streamed_rows']:,} | stream+gcs={stats['export_parquet_sec']}s | "
            f"bq_load={stats['native_copy_sec']}s | biglake={stats['biglake_mount_sec']}s | "
            f"shards={stats['parquet_shards']}"
        )
    return results


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cross-region TPC-DS small_1g ingestion via BigQuery Storage Read API."
    )
    parser.add_argument("--project-id", default=None, help="Target GCP PROJECT_ID (defaults to .env)")
    parser.add_argument("--bq-location", default=None, help="Target BigQuery region (defaults to .env)")
    parser.add_argument("--bench-bucket", default=None, help="Target regional GCS bucket (defaults to .env)")
    parser.add_argument("--scale-preset", default=None, help="Must be small_1g when BQ_LOCATION != US")
    parser.add_argument(
        "--gcs-prefix",
        default=None,
        help="GCS key prefix under BENCH_BUCKET (default: bronze/<scale_preset>)",
    )
    parser.add_argument("--ds-bronze-native", default=None)
    parser.add_argument("--ds-bronze-biglake", default=None)
    parser.add_argument("--biglake-connection-id", default=None)
    parser.add_argument(
        "--ingest-mode",
        choices=("both", "native_only"),
        default=None,
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated subset of TPC-DS tables (defaults to all 8 Bronze tables)",
    )
    parser.add_argument("--max-stream-count", type=int, default=4)
    parser.add_argument("--rows-per-shard", type=int, default=1_000_000)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate region/scale compatibility and print resolved targets without calling GCP APIs",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    cfg = resolve_bench_config(repo_root=REPO_ROOT, populate_environ=False)

    project_id = (args.project_id or cfg.get("PROJECT_ID") or "").strip()
    bq_location = (args.bq_location or cfg.get("BQ_LOCATION") or "US").strip()
    scale_preset = (args.scale_preset or cfg.get("SCALE_PRESET") or "small_1g").strip()
    bench_bucket = (args.bench_bucket or cfg.get("BENCH_BUCKET") or "").strip()
    ds_bronze_native = (args.ds_bronze_native or cfg.get("DS_BRONZE_NATIVE") or "bench_bronze_native").strip()
    ds_bronze_biglake = (args.ds_bronze_biglake or cfg.get("DS_BRONZE_BIGLAKE") or "bench_bronze_biglake").strip()
    cfg_loc = (cfg.get("BQ_LOCATION") or "US").strip()
    default_cfg_conn = f"bench-biglake-{cfg_loc.lower()}"
    cfg_conn = (cfg.get("BIGLAKE_CONNECTION_ID") or "").strip()
    if args.biglake_connection_id:
        conn_id = args.biglake_connection_id.strip()
    elif cfg_conn and (bq_location.lower() == cfg_loc.lower() or cfg_conn != default_cfg_conn):
        conn_id = cfg_conn
    else:
        conn_id = f"bench-biglake-{bq_location.lower()}"
    ingest_mode = (args.ingest_mode or cfg.get("INGEST_MODE") or "both").strip().lower()
    gcs_prefix = (args.gcs_prefix or f"bronze/{scale_preset}").strip("/")
    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None

    try:
        validate_cross_region_request(bq_location=bq_location, scale_preset=scale_preset)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("DRY RUN OK:")
        print(f"  project_id={project_id or '(unset)'}")
        print(f"  bq_location={bq_location}")
        print(f"  scale_preset={scale_preset}")
        print(f"  gcs_prefix=gs://{bench_bucket or '<BENCH_BUCKET>'}/{gcs_prefix}/<table>/")
        print(f"  ds_bronze_native={ds_bronze_native}")
        print(f"  ds_bronze_biglake={ds_bronze_biglake} (conn={conn_id})")
        print(f"  tables={tables or TPCDS_TABLES}")
        return 0

    try:
        stage_tpcds_cross_region(
            project_id=project_id,
            bq_location=bq_location,
            bench_bucket=bench_bucket,
            scale_preset=scale_preset,
            ds_bronze_native=ds_bronze_native,
            ds_bronze_biglake=ds_bronze_biglake,
            biglake_connection_id=conn_id,
            gcs_prefix=gcs_prefix,
            ingest_mode=ingest_mode,
            tables=tables,
            max_stream_count=args.max_stream_count,
            rows_per_shard=args.rows_per_shard,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
