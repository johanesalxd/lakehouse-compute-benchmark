# Lakehouse Compute Benchmark — Architecture & Benchmark Contract (`SPEC.md`)

> **Purpose:** Single source of truth governing data ingestion, Medallion transformation parity, fairness guardrails, telemetry schema, and zero-collision file ownership across all benchmark variants. Every notebook and helper script **MUST** conform strictly to this specification so results across engines are 100% apples-to-apples.

---

## 1. Benchmark Scope & Non-Negotiable Fairness Guardrails

1. **100% Generic & Public-Safe Repository:**
   - Zero customer names, internal URLs, internal pricing/discount codes, or hardcoded GCP project IDs anywhere in code, markdown, comments, or committed notebook outputs. Read all environment settings via `.env` / `os.environ`.
2. **Zero BigQuery Pushdown on Spark Variants (`01` & `02`):**
   - Notebooks `01_medallion_spark_standard.ipynb` and `02_medallion_spark_lightning.ipynb` **MUST NOT** use `spark-bigquery-connector` to read or filter source tables.
   - All Spark reads and Medallion writes (`Bronze -> Silver -> Gold`) execute purely against **open Parquet files in Cloud Storage (`gs://${BENCH_BUCKET}/...`)** using the Spark DataFrame / Spark SQL API so in-cluster JVM vs. Lightning Engine + Gluten/Velox C++ (`NQE`) compute is measured honestly.
3. **x86 Hardware Parity on Dataproc GCE (`Image 2.3-debian12`):**
   - Both `CLUSTER_STD_NAME` (`--engine=default`) and `CLUSTER_LGT_NAME` (`--engine=lightning`) use identical **x86** GCE VM shapes (`MASTER_MACHINE_TYPE`, `WORKER_MACHINE_TYPE`, `NUM_PRIMARY_WORKERS`, `NUM_WORKER_LOCAL_SSDS` NVMe Local SSDs; zero ARM VMs).
4. **BigQuery `BigFrames 2.x` (`03`) Grounding, Optimization Contrasts & Slot Cap:**
   - Use official **`bigframes.pandas as bpd`** (`bigframes >= 2.40`; **do NOT import non-existent `bigframes.spark`**).
   - Explicitly set **`bpd.options.bigquery.ordering_mode = "partial"`** at session initialization so `BigFrames` uses distributed unordered SQL pushdown rather than hashing every row for strict sequential pandas indexing.
   - Run `03` across **four BigQuery execution passes** (`BF_PASS_FILTER="all"` default, or comma-separated subset from `biglake,native,ctas_native,lazy_e2e`) separating **Storage Engine**, **Write/Clustering Optimization**, and **Pipeline Fusion**:
     - **Pass 4 — `bigframes_biglake_gcs`:** Queries `DS_BRONZE_BIGLAKE` external BigLake Parquet tables pointing at the **exact same GCS Parquet files** read by Spark (`to_gbq` into `DS_SILVER_BQ` for downstream stages).
     - **Pass 5 — `bigframes_native_bq`:** Queries `DS_BRONZE_NATIVE` (`storage_billing_model = 'PHYSICAL'` Capacitor tables) using naive `df.to_gbq(..., if_exists="replace")` per stage (baseline demonstrating staged `_SESSION.bqdf_*` + `MERGE INTO` double-writes and unclustered Silver intermediates).
     - **Pass 6 — `bigframes_ctas_native_bq`:** Materializes identical 4-stage Silver & Gold checkpoints via single-pass `df.sql` **`CREATE OR REPLACE TABLE ... CLUSTER BY ...`** (`sold_date_sk, customer_sk, item_sk` on Silver; `d_date_sk, channel, i_category` on Gold) and omits Stage 3 `daily.cache()`, eliminating `_SESSION + MERGE` overhead as a within-BigQuery ELT optimization while keeping 100% 4-stage checkpoint symmetry with Spark.
     - **Pass 7 — `bigframes_lazy_e2e_bq` (`stage_id = "stage_e2e_bronze_to_gold"`):** Chains Stage 1 -> Stage 2A -> Stage 3 lazily in ONE `BigFrames` expression over `DS_BRONZE_NATIVE` and materializes Gold in a single Clustered CTAS with zero intermediate Silver disk writes — categorized explicitly as **Pipeline Fusion (no Silver materialization)** for Gold parity, slot-second savings, and TCO contrast rather than 1:1 checkpoint-equivalent engine speed.
   - Demonstrate BigQuery slot governance live: verify `SET @@reservation = 'NONE'` is rejected (`HTTP 400`) when assigned to `BQ_RESERVATION_NAME` without override permissions (`ALLOW_UNRESERVED_BQ=true` overrides if unreserved), and capture exact `total_slot_ms`, `total_bytes_processed`, and `bi_engine_statistics` from `` `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT ``.
5. **Scale-Preset Portability (`small_1g` First -> Zero-Code Scale-Up to `100g` / `500g`):**
   - All data sits in **`US` multi-region** (`BQ_LOCATION="US"`, `GCS_LOCATION="US"`) paired with Dataproc GCE in **`us-central1`** (`$0.00` intra-US GCS egress).
   - Reference multi-scale hardware progression: **`small_1g`** (`16` Spark worker vCPUs / `2` NVMe SSDs vs. `100` BQ slots) -> **`medium_100g`** (`64` Spark worker vCPUs / `8` NVMe SSDs vs. `200` BQ slots) -> **`large_500g`** (`64` Spark worker vCPUs / `8` NVMe SSDs vs. `400` BQ slots); `00` supports optional `INGEST_MODE="native_only"` for fast `DS_BRONZE_NATIVE` backfills. Changing `SCALE_PRESET` in `.env` requires re-running `00` only; notebooks `01`–`04` require **zero code edits**.

---

## 2. Source Public Dataset & `SCALE_PRESET` Definitions (`TPC-DS`)

All 8 required tables are openly readable (`allUsers`) in `bigquery-public-data.tpc_ds_1g` and `bigquery-public-data.tpc_ds_10t` (`location = US`):

| Table | Role | Scale Behavior (`small_1g` vs. `medium_100g` / `large_500g`) | Clustering / Partition Key |
| :--- | :--- | :--- | :--- |
| `store_sales` | Fact (`23` cols) | Full table in `tpc_ds_1g` (`2.88M` rows); `ss_sold_date_sk` window in `tpc_ds_10t` | `ss_sold_date_sk` |
| `store_returns` | Fact (`20` cols) | Full table in `tpc_ds_1g`; `sr_returned_date_sk` window in `tpc_ds_10t` | `sr_returned_date_sk` |
| `web_sales` | Fact (`34` cols) | Full table in `tpc_ds_1g`; `ws_sold_date_sk` window in `tpc_ds_10t` | `ws_sold_date_sk` |
| `web_returns` | Fact (`24` cols) | Full table in `tpc_ds_1g`; `wr_returned_date_sk` window in `tpc_ds_10t` | `wr_returned_date_sk` |
| `customer` | Dim (`18` cols) | Full table (`100K` rows in `1g`; `65M` rows in `10t`, filtered to active `customer_sk` in slice if `medium_100g`) | `c_customer_sk` |
| `customer_demographics` | Dim (`9` cols) | Full table (`1.92M` rows) | `cd_demo_sk` |
| `item` | Dim (`22` cols) | Full table (`18K` rows in `1g`; `402K` rows in `10t`) | `i_item_sk` |
| `date_dim` | Dim (`28` cols) | Full table (`73,049` rows) | `d_date_sk` |

### Exact `SCALE_PRESET` Filter Rules in `00_data_copy_ingestion.ipynb`
- **`small_1g` (Default E2E Validation):** Source = `bigquery-public-data.tpc_ds_1g`, no date filter (`100%` of rows).
- **`medium_100g` (`~100 GB` Logical Slice):** Source = `bigquery-public-data.tpc_ds_10t`, fact date-key filter `BETWEEN 2452276 AND 2452293` (`18` days in `2002`, leveraging native `*_date_sk` clustering on `tpc_ds_10t`; `customer` filtered via `WHERE c_customer_sk IN (SELECT DISTINCT customer_sk FROM sliced_facts)` so dimension size stays proportional).
- **`large_500g` (`~500 GB` Logical Slice):** Source = `bigquery-public-data.tpc_ds_10t`, fact date-key filter `BETWEEN 2452276 AND 2452365` (`90` days in `2002`).

### Verified TPC-DS Column Names Used Across Stages
- `store_sales`: `ss_sold_date_sk`, `ss_item_sk`, `ss_customer_sk`, `ss_cdemo_sk`, `ss_ticket_number`, `ss_quantity`, `ss_wholesale_cost`, `ss_list_price`, `ss_sales_price`, `ss_ext_discount_amt`, `ss_ext_sales_price`, `ss_ext_wholesale_cost`, `ss_net_paid`, `ss_net_profit`
- `store_returns`: `sr_returned_date_sk`, `sr_item_sk`, `sr_customer_sk`, `sr_ticket_number`, `sr_return_quantity`, `sr_return_amt`, `sr_net_loss`
- `web_sales`: `ws_sold_date_sk`, `ws_item_sk`, `ws_bill_customer_sk`, `ws_bill_cdemo_sk`, `ws_order_number`, `ws_quantity`, `ws_wholesale_cost`, `ws_list_price`, `ws_sales_price`, `ws_ext_discount_amt`, `ws_ext_sales_price`, `ws_ext_wholesale_cost`, `ws_net_paid`, `ws_net_profit`
- `web_returns`: `wr_returned_date_sk`, `wr_item_sk`, `wr_refunded_customer_sk`, `wr_order_number`, `wr_return_quantity`, `wr_return_amt`, `wr_net_loss`
- `customer`: `c_customer_sk`, `c_customer_id`, `c_current_cdemo_sk`, `c_preferred_cust_flag`, `c_birth_year`, `c_birth_country`
- `customer_demographics`: `cd_demo_sk`, `cd_gender`, `cd_marital_status`, `cd_education_status`, `cd_credit_rating`, `cd_dep_count`
- `item`: `i_item_sk`, `i_item_id`, `i_brand`, `i_class`, `i_category`, `i_current_price`, `i_wholesale_cost`
- `date_dim`: `d_date_sk`, `d_date`, `d_year`, `d_moy`, `d_qoy`, `d_weekend`, `d_holiday`

---

## 3. Identical Medallion Specification (`01`, `02`, `03`)

Every staged compute pass (`01` Standard Spark, `02` Lightning Base + Lightning NQE, `03` BigFrames `bigframes_biglake_gcs`, `bigframes_native_bq`, and `bigframes_ctas_native_bq`) **MUST** execute and time these exact 3 stages (plus optional Stage `2B` Python UDF / SQL pushdown contrast), while `bigframes_lazy_e2e_bq` chains Stages `1 -> 2A -> 3` lazily into `stage_e2e_bronze_to_gold` as a **Pipeline Fusion (no Silver materialization)** pass:

### Stage 1: `stage1_bronze_to_silver` (Omnichannel Sales & Returns Reconciliation)
1. **Filter clean sales lines:** Keep rows where `sold_date_sk IS NOT NULL`, `item_sk IS NOT NULL`, `customer_sk IS NOT NULL`, and `quantity > 0` (`ws_bill_customer_sk` & `ws_bill_cdemo_sk` for web).
2. **Pre-aggregate returns by `(item_sk, order_or_ticket_id)`** before left-joining (prevents fan-out if a ticket/order has multiple return rows!):
   - `store_returns_agg`: group by `(sr_item_sk, sr_ticket_number)` -> `SUM(COALESCE(sr_return_quantity, 0)) AS return_qty`, `SUM(COALESCE(sr_return_amt, 0.0)) AS return_amt`, `SUM(COALESCE(sr_net_loss, 0.0)) AS return_net_loss`.
   - `web_returns_agg`: group by `(wr_item_sk, wr_order_number)` -> `SUM(COALESCE(wr_return_quantity, 0)) AS return_qty`, `SUM(COALESCE(wr_return_amt, 0.0)) AS return_amt`, `SUM(COALESCE(wr_net_loss, 0.0)) AS return_net_loss`.
3. **Left-join & Union (`store` + `web`)** into materialized Silver table / Parquet dataset `silver_omnichannel_sales` with exact columns:
   - `channel` (`'STORE'` or `'WEB'`), `sold_date_sk` (`INT64`), `item_sk` (`INT64`), `customer_sk` (`INT64`), `cdemo_sk` (`INT64`), `order_number` (`INT64`), `quantity` (`INT64`), `gross_sales_price` (`FLOAT64`, from `COALESCE(ext_sales_price, 0.0)`), `discount_amt` (`FLOAT64`, from `COALESCE(ext_discount_amt, 0.0)`), `net_paid` (`FLOAT64`, from `COALESCE(ss_net_paid/ws_net_paid, 0.0)`), `net_profit` (`FLOAT64`, from `COALESCE(ss_net_profit/ws_net_profit, 0.0)`), `return_qty` (`INT64`, default `0`), `return_amt` (`FLOAT64`, default `0.0`), `net_paid_after_return` (`FLOAT64` = `net_paid - COALESCE(return_amt, 0.0)`), `net_profit_after_return` (`FLOAT64` = `net_profit - COALESCE(return_net_loss, 0.0)`).
4. **Materialize** `silver_omnichannel_sales` (`mode="overwrite"` Parquet on GCS for Spark; `to_gbq(..., if_exists="replace")` in `DS_SILVER_BQ` for baseline `BigFrames`; single-pass `CREATE OR REPLACE TABLE ... CLUSTER BY sold_date_sk, customer_sk, item_sk` for `bigframes_ctas_native_bq`) so Stage 1 write I/O is captured cleanly and downstream stages read materialized Silver. (Omitted in `bigframes_lazy_e2e_bq`.)

### Stage 2A (`stage2a_silver_vectorized`) & Stage 2B (`stage2b_silver_udf`): Tiering & Risk Scoring
Computes `order_risk_tier` (`STRING`) and `adjusted_margin_score` (`FLOAT64`) over `silver_omnichannel_sales` (on `medium_100g`/`large_500g`, Stage `2B` filters deterministically to `MOD(order_number, 20) = 0` or `UDF_MAX_SAMPLE_ROWS` and logs `sample_fraction` so UDF overhead is quantified without multi-hour runs; on `small_1g`, run on `100%` of Silver rows or deterministic `500K` cap):
- **Exact Deterministic Tiering Rule:**
  1. If `return_amt > 0.5 * gross_sales_price` and `return_qty > 0` -> `'HIGH_RETURN_RISK'`
  2. Else if `net_profit_after_return > 50.0` and `discount_amt == 0.0` -> `'VIP_FULL_PRICE'`
  3. Else if `discount_amt > 0.25 * gross_sales_price` -> `'PROMO_DRIVEN'`
  4. Else -> `'STANDARD'`
- **`adjusted_margin_score`:** `ROUND(net_profit_after_return - (0.15 * return_amt) + (0.05 * net_paid_after_return), 4)`.
- **Stage 2A (`vectorized`):** Built-in Spark Catalyst/Velox `F.when` expressions vs. `BigFrames` native vectorized Series boolean masks (`bpd` / `case_when`) -> materialize `silver_enriched_sales`.
- **Stage 2B (`python_udf`):** Same exact Python branching function executed as a PySpark `@F.udf` (demonstrating JVM/Python pickle overhead + Gluten/Velox `NQE` fallback to JVM!) vs. `BigFrames` custom function / SQL pushdown contrast -> materialize/count `silver_udf_benchmark_out`.

### Stage 3: `stage3_silver_to_gold` (5-Way Star Join + Rolling 7d/30d Windows + Exact Distincts)
1. **5-Way Star Join:** Join `silver_enriched_sales s` with:
   - `customer c` on `s.customer_sk = c.c_customer_sk`
   - `customer_demographics cd` on `COALESCE(s.cdemo_sk, c.c_current_cdemo_sk) = cd.cd_demo_sk`
   - `item i` on `s.item_sk = i.i_item_sk`
   - `date_dim d` on `s.sold_date_sk = d.d_date_sk`
   *(Filter `i.i_category IS NOT NULL AND cd.cd_credit_rating IS NOT NULL AND d.d_date IS NOT NULL`).*
2. **Daily Grain Aggregation (`daily_category_channel_metrics`):**
   Group by `(d_date_sk, d_date, d_year, d_moy, channel, i_category, cd_credit_rating, order_risk_tier)` ->
   - `order_lines = COUNT(*)`
   - `unique_customers = COUNT(DISTINCT s.customer_sk)`
   - `daily_gross_sales = SUM(gross_sales_price)`
   - `daily_return_amt = SUM(return_amt)`
   - `daily_net_paid = SUM(net_paid_after_return)`
   - `daily_net_profit = SUM(net_profit_after_return)`
   - `avg_margin_score = AVG(adjusted_margin_score)`
3. **Rolling Window Analytics (Final `gold_omnichannel_kpi_mart`):**
   Partition by `(channel, i_category, cd_credit_rating, order_risk_tier)` ordered by `d_date_sk ASC`:
   - `rolling_7d_net_paid`: `SUM(daily_net_paid)` over `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`
   - `rolling_30d_net_paid`: `SUM(daily_net_paid)` over `ROWS BETWEEN 29 PRECEDING AND CURRENT ROW`
   - `rolling_30d_return_rate`: `SAFE_DIVIDE(SUM(daily_return_amt) OVER 29d, NULLIF(SUM(daily_gross_sales) OVER 29d, 0))`
   - `category_daily_share`: `daily_net_paid / NULLIF(SUM(daily_net_paid) OVER (PARTITION BY d_date_sk, channel), 0)`
4. **Materialize** `gold_omnichannel_kpi_mart` (or `gold_omnichannel_kpi_mart_lazy_e2e` via **`stage_e2e_bronze_to_gold`** in `bigframes_lazy_e2e_bq`, which executes Steps `Stage 1 -> Stage 2A -> Stage 3` in one Clustered CTAS without intermediate Silver disk writes) and record validation checksums: `COUNT(*)` and `ROUND(SUM(daily_net_paid), 2)`. Across all **7 variants** (`01`, `02`, `03`), Final Gold KPI Mart **`COUNT(*)` MUST match 100% and `SUM(daily_net_paid)` MUST match within `±0.01%` floating-point tolerance!**

---

## 4. Unified Telemetry Schema (`results/benchmark_runs.jsonl` & `${DS_METRICS}.benchmark_runs`)

`scripts/metrics_helper.py` provides a tiny stdlib-first logger (`log_stage_metric(...)`) writing both to local `results/benchmark_runs.jsonl` (and `results/sample_benchmark_runs.jsonl` for dry-run visualization) and optionally appending to BigQuery `${PROJECT_ID}.${DS_METRICS}.benchmark_runs`:

```json
{
  "run_id": "20261116T080000Z_small_1g",
  "timestamp_utc": "2026-11-16T08:00:12Z",
  "scale_preset": "small_1g",
  "engine_variant": "spark_standard | spark_lightning_base | spark_lightning_nqe | bigframes_biglake_gcs | bigframes_native_bq | bigframes_ctas_native_bq | bigframes_lazy_e2e_bq",
  "storage_backend": "gcs_parquet | biglake_gcs_parquet | bq_native_capacitor",
  "stage_id": "stage1_bronze_to_silver | stage2a_silver_vectorized | stage2b_silver_udf | stage3_silver_to_gold | stage_e2e_bronze_to_gold",
  "start_epoch_sec": 1794816000.12,
  "end_epoch_sec": 1794816018.45,
  "wall_clock_sec": 18.33,
  "output_row_count": 3598412,
  "checksum_net_paid": 128493021.44,
  "worker_machine_type": "n2d-standard-8",
  "total_worker_vcpus": 16,
  "vcpu_seconds": 293.28,
  "bq_slot_cap": null,
  "bq_total_slot_ms": null,
  "bq_bytes_processed": null,
  "nqe_enabled": false,
  "nqe_fallback_note": "",
  "est_list_cost_usd": 0.0812,
  "sample_fraction": 1.0
}
```

### Cost Calculation Formulas in `04_benchmark_summary_and_tco.ipynb` (Parameterized US List Rates)
Keep all rates parameterized at the top of `04_benchmark_summary_and_tco.ipynb` so users can plug in any region or CUD discount (`1-Yr` / `3-Yr` / `Spot`):
- **Dataproc GCE Standard (`us-central1` default list parameters):** `(VM $/vCPU-hr + $0.010 Dataproc fee + Local SSD $/hr) × cluster_hours` (automatically inferring per-scale worker & NVMe SSD count: `2` SSDs on `2 x n2d-standard-8`, `8` SSDs on `4 x n2d-standard-16`; plus parameterized toggle comparing **On-Demand** vs. **`15%` Primary 1-Yr CUD + `85%` Spot EFM**!).
- **Dataproc GCE Lightning Engine:** Adds **`+$0.0025 / vCPU-hr`** Lightning Engine surcharge on top of Standard, calculating the empirical **break-even speedup `%`** (`~5–10%` on On-Demand VMs, `~36%` on heavy Spot blends) vs. actual measured speedup!
- **BigQuery Enterprise Edition (`US` multi-region):** Models both **(a) Shared / Consumed Slot-Hour Allocation** (`slot_seconds_used / 3,600 × $0.06/slot-hr` list or `$0.048` 1-Yr / `$0.036` 3-Yr CUD, reflecting immediate idle-slot return in `0s`) and **(b) Standalone Reservation Ceiling Cost** (`BQ_SLOT_CAP × wall_clock_hours × slot_rate`, e.g., `100` / `200` / `400` slots). Empirical **`1 BigQuery Slot ≈ X Spark vCPUs`** equivalency ratios are anchored on 1:1 Silver-materializing passes (`bigframes_biglake_gcs` Stage 1 and staged Native BQ `to_gbq` / `CTAS`), while **`bigframes_lazy_e2e_bq`** (`stage_e2e_bronze_to_gold`) is labeled explicitly as **Pipeline Fusion (no Silver writes)** alongside exact `%` slot-second and TCO reductions!

---

## 5. Component & Module Ownership

| Component | Role | Owned Files |
| :--- | :--- | :--- |
| **Infrastructure & Bronze Ingestion** | GCP provisioning, teardown, shared 21-field telemetry helper, and Bronze TPC-DS dual-backend copy | `README.md`, `scripts/provision_infra.sh`, `scripts/teardown_infra.sh`, `scripts/metrics_helper.py`, `notebooks/00_data_copy_ingestion.ipynb` |
| **Dataproc Spark Standard** | Shared pure-GCS PySpark Medallion runner and Standard Spark (`--engine=default`) notebook | `notebooks/01_medallion_spark_standard.ipynb`, `scripts/run_spark_medallion.py` |
| **Dataproc Spark Lightning** | Lightning Base (`runtime=default`) and Gluten/Velox C++ NQE (`runtime=native`) execution + plan inspection | `notebooks/02_medallion_spark_lightning.ipynb` |
| **BigQuery DataFrames (`BigFrames`)** | `bigframes.pandas` (`ordering_mode="partial"`) over `DS_BRONZE_BIGLAKE` & `DS_BRONZE_NATIVE` + slot reservation governance | `notebooks/03_medallion_bigquery_bigframes.ipynb` |
| **Executive Summary & TCO** | Deterministic dry-run sample telemetry, parity audit, latency charts, and parameterized TCO model | `notebooks/04_benchmark_summary_and_tco.ipynb`, `scripts/generate_sample_telemetry.py`, `results/sample_benchmark_runs.jsonl` |
