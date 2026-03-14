# 🏭 FMCG Merger Data Pipeline — Unified Analytics on Databricks (AWS S3 + Medallion Architecture)

> **End-to-end ELT pipeline built on Databricks to unify data from a fictional FMCG merger between Atlon (sports equipment) and Sports Bar (energy bars), processing messy multi-source data into a single BI-ready Star Schema served via Databricks Dashboards and Genie AI.**

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Walkthrough](#pipeline-walkthrough)
  - [Catalog & Schema Setup](#catalog--schema-setup)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Data Cleansing & Transformation](#silver-layer--data-cleansing--transformation)
  - [Gold Layer — BI-Ready & Merge with Parent](#gold-layer--bi-ready--merge-with-parent)
  - [Fact Table — Full Load & Incremental Load](#fact-table--full-load--incremental-load)
- [Data Model](#data-model)
- [Orchestration](#orchestration)
- [Analytics Dashboard](#analytics-dashboard)
- [Key Metrics](#key-metrics)
- [Setup & Usage](#setup--usage)
- [Learnings & Highlights](#learnings--highlights)

---

## Project Overview

This project simulates a real-world **post-merger data integration challenge** between two FMCG companies:

- **Atlon** — a mature sports equipment company with an existing, structured data infrastructure
- **Sports Bar** — a fast-growing energy bar startup with messy data in spreadsheets and unstructured formats

The goal is to resolve the resulting "data chaos" by building a scalable, unified ELT pipeline that meets three success criteria:

1. **Unified Analytics** — aggregated metrics for both companies in a single, reliable dashboard
2. **Low Learning Curve** — clean, modular notebooks that new hires can follow quickly
3. **Scalability** — a Medallion Architecture foundation that supports long-term growth

Sports Bar's raw CSV data is ingested from **AWS S3**, transformed through Bronze → Silver → Gold layers in **Databricks**, and merged into Atlon's existing Gold tables. The final serving layer powers a **Databricks BI Dashboard** and a **Databricks Genie** AI assistant.

---

## Architecture

```
Sports Bar Raw Data (CSV)
        │
        ▼
  AWS S3 Bucket
  ┌─────────────────────────────┐
  │  /landing/  →  /processed/  │
  └─────────────────────────────┘
        │
        │  External Connection
        ▼
  Databricks (Lakeflow Jobs)
  ┌──────────────────────────────────────────────────┐
  │  Unity Catalog — "fmcg"                          │
  │                                                  │
  │  Child Company Pipeline:                         │
  │  Bronze  →  Silver  →  Child Gold (sb_*)         │
  │                             │                    │
  │                             │  MERGE INTO        │
  │                             ▼                    │
  │                    Parent Gold (Atlon)            │
  └──────────────────────────────────────────────────┘
        │
        ▼
  Serving Layer
  ┌──────────────────────────┐
  │  Databricks Dashboard    │
  │  Databricks Genie (AI)   │
  └──────────────────────────┘
```

> See `project_architecture.png` for the full visual diagram.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | AWS |
| Storage | AWS S3 (landing + archive pattern) |
| Compute & Processing | Databricks (Free Edition) |
| Data Format | Delta Lake (ACID, Change Data Feed) |
| Catalog & Governance | Unity Catalog |
| Language | Python (PySpark), SQL |
| Orchestration | Databricks Lakeflow Jobs |
| BI & Reporting | Databricks Dashboard + Databricks Genie (AI) |

---

## Project Structure

```
📦 fmcg-merger-databricks-pipeline
 ┣ 📄 1_customer_data_Processing.py     # Customers: Bronze → Silver → Gold → Merge
 ┣ 📄 2_products_data_processing.py     # Products: Bronze → Silver → Gold → Merge
 ┣ 📄 3_pricing_data_processing.py      # Gross Price: Bronze → Silver → Gold → Merge
 ┣ 📄 1_full_load_fact.py               # Fact Orders: Historical 5-month backfill
 ┣ 📄 2_incremental_load_fact.py        # Fact Orders: Incremental daily load + staging
 ┣ 🖼️ project_architecture.png          # Pipeline architecture diagram
 ┗ 🖼️ fmcg_dashboard.pdf               # Databricks BI dashboard screenshot
```

---

## Pipeline Walkthrough

### Catalog & Schema Setup

A new Unity Catalog named `fmcg` is created with three schemas:

```sql
CREATE CATALOG IF NOT EXISTS fmcg;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
```

Atlon's (parent company) existing data is imported directly into the Gold layer. Sports Bar's (child company) data flows through the full Medallion pipeline before merging.

---

### Bronze Layer — Raw Ingestion

Raw CSV files are loaded from S3 with file metadata captured for lineage tracking:

```python
df = spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size", "_metadata.file_path")
```

Change Data Feed (`delta.enableChangeDataFeed`) is enabled on all Delta tables to support downstream incremental processing.

---

### Silver Layer — Data Cleansing & Transformation

Extensive ETL is applied per entity to align Sports Bar's data with Atlon's schema.

**Customers (`1_customer_data_Processing.py`):**
- Deduplicated on `customer_id`
- Trimmed whitespace; applied `initcap` for consistent title casing on `customer_name`
- Fixed city typos via replacement dictionary (`Bengaluruu → Bengaluru`, `Hyderabadd → Hyderabad`, `NewDelhi → New Delhi`)
- Patched 4 customers with null cities using business-confirmed corrections via a lookup join + `coalesce`
- Derived new columns to match parent schema: `customer` (name-city concat), `market`, `platform`, `channel`

**Products (`2_products_data_processing.py`):**
- Deduplicated on `product_id`; fixed title casing on `category`
- Corrected spelling mistake (`Protien → Protein`) using `regexp_replace`
- Added `division` column mapped from category values
- Extracted `variant` from product name using regex: `r"\((.*?)\)"`
- Generated a deterministic `product_code` using **SHA-256 hash** on `product_name`
- Sanitised invalid `product_id` values (non-numeric) → replaced with fallback `999999`

**Gross Price (`3_pricing_data_processing.py`):**
- Normalised inconsistent date formats across 4 patterns using `try_to_date` + `coalesce`
- Fixed negative prices using regex validation; replaced non-numeric values with `0`
- Joined with Silver products to resolve `product_code` for each `product_id`
- Used a **Window Function** (`row_number()`) to select the latest non-zero monthly price per product per year, converting Sports Bar's monthly pricing into Atlon's yearly pricing format

---

### Gold Layer — BI-Ready & Merge with Parent

Each Silver table is promoted to a child Gold table (`sb_dim_*`) and then **merged into Atlon's parent Gold tables** using Delta Lake `MERGE INTO`:

```python
delta_table.alias("target").merge(
    source=df_child.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(set={...})
 .whenNotMatchedInsert(values={...})
 .execute()
```

This ensures parent Gold tables (`dim_customers`, `dim_products`, `dim_gross_price`) are enriched with Sports Bar records without overwriting existing Atlon data.

---

### Fact Table — Full Load & Incremental Load

**Full Load (`1_full_load_fact.py`):**
- Ingests 5 months of historical order CSVs from the S3 `/landing/` folder
- Cleans data: drops null `order_qty`, sanitises non-numeric `customer_id` → `999999`, strips weekday names from date strings, parses dates across 4 formats
- Joins with Silver products to resolve `product_code`
- Aggregates **daily → monthly** using `trunc(date, "MM")` + `groupBy` to match Atlon's monthly reporting grain
- Merges into parent `fact_orders` Gold table on `(date, product_code, customer_code)`
- Archives processed files from `/landing/` to `/processed/` in S3

**Incremental Load (`2_incremental_load_fact.py`):**
- Reads only new files from `/landing/` on each run
- Writes to a **staging Bronze table** (overwrite) to isolate incremental data from full history
- Applies the same Silver transformations and MERGE upsert into Silver + child Gold tables
- Recalculates monthly totals only for **affected months** using an `incremental_months` temp view — avoiding full history reprocessing
- Merges recalculated monthly rows into parent `fact_orders` Gold table
- Cleans up staging tables (`DROP TABLE`) after each run

---

## Data Model

The Gold layer follows a **Star Schema** design:

```
dim_date ──────────────────────────────┐
dim_customers ─────────────────────────┤
dim_products ──────────────────────────┤──── fact_orders
dim_gross_price ───────────────────────┘
```

| Table | Description |
|---|---|
| `fact_orders` | Monthly order quantities per customer + product (parent + child merged) |
| `dim_customers` | Unified customers from Atlon + Sports Bar |
| `dim_products` | Products with division, category, variant, SHA-256 product_code |
| `dim_gross_price` | Yearly price per product (resolved from Sports Bar's monthly data) |
| `dim_date` | Generated date dimension for quarterly/monthly time-based analytics |

---

## Orchestration

A **Databricks Lakeflow Job** sequences all pipeline notebooks in dependency order:

```
Customers → Products → Gross Price → Orders (Full / Incremental)
```

The job is scheduled to run **nightly** for automated incremental refreshes. Each notebook accepts a `catalog` widget parameter for multi-environment deployments (`dev`, `staging`, `prod`).

---

## Analytics Dashboard

The Databricks BI Dashboard connects directly to the unified Gold layer and surfaces:

| KPI | Value |
|---|---|
| Total Revenue | **₹105.34 Billion** |
| Total Quantity Sold | **34.13 Million** |
| Unique Customers | **54** |
| Average Selling Price | **₹4,043.16** |

**Visuals included:**
- 📊 Top 10 Products by Revenue (horizontal bar chart)
- 🍩 Revenue Share by Channel — Retailer (78.49%) vs Direct vs Acquisition
- 📈 Monthly Revenue Trend (Jan → Dec)
- 📋 All Customers by Revenue (quantity + revenue table)
- 📊 Top Variant by Revenue
- 🔵 Product Price vs Quantity (bubble scatter chart)
- 🔽 Slicers for Year, Quarter, Month, Channel, Category

**Databricks Genie** enables natural language queries on the Gold layer — e.g., *"Top 5 products by revenue"* — returning instant SQL-generated answers and visualisations without writing a single line of code.

---

## Setup & Usage

### Prerequisites

- Databricks workspace (Free Edition or above)
- AWS S3 bucket with External Connection configured in Databricks
- Unity Catalog enabled on the Databricks workspace

### Steps

1. **Upload source CSVs** to S3 landing paths:
   ```
   s3://your-bucket/full_load/customers/
   s3://your-bucket/full_load/products/
   s3://your-bucket/full_load/gross_price/
   s3://your-bucket/full_load/orders/landing/
   ```

2. **Create the `utilities` notebook** at `/Workspace/.../01_Setup/utilities` defining `bronze_schema`, `silver_schema`, `gold_schema` variables

3. **Run notebooks in order:**
   ```
   1_customer_data_Processing.py
   2_products_data_processing.py
   3_pricing_data_processing.py
   1_full_load_fact.py          ← first time only (historical backfill)
   2_incremental_load_fact.py   ← all subsequent daily runs
   ```

4. **Set up Lakeflow Job** to sequence and schedule the incremental pipeline nightly

5. **Connect Databricks Dashboard** to `fmcg.gold` schema and enable Genie on the Gold tables

### Widget Parameters

All notebooks accept runtime parameters:

```python
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")
```

---

## Learnings & Highlights

- ✅ **Post-merger schema unification** — merging two companies' schemas with different grains, formats, and conventions into one unified Gold layer
- ✅ **Staging table pattern** — isolating incremental data before merging to avoid full re-scans of historical data
- ✅ **Landing → Processed file archiving** — S3 file movement after ingestion to prevent double-processing
- ✅ **SHA-256 surrogate key generation** — deterministic `product_code` from `product_name` for reliable cross-company joins
- ✅ **Window Function for price resolution** — `row_number()` over `(product_code, year)` to select the latest non-zero monthly price, bridging daily/monthly vs yearly pricing models
- ✅ **Multi-format date parsing** — `try_to_date` + `coalesce` across 4 date formats for resilient ingestion of messy source data
- ✅ **Grain alignment** — aggregating Sports Bar's daily orders to monthly totals before merging into Atlon's monthly `fact_orders`
- ✅ **Change Data Feed** — enabled on all Delta tables for efficient incremental downstream processing
- ✅ **Databricks Genie** — AI-powered natural language querying as a zero-code analytics interface on top of the Gold layer

---

## Author

Built as part of a hands-on Databricks data engineering project simulating a real-world FMCG merger integration scenario.  
Pipeline orchestrated on **Databricks** | Storage on **AWS S3** | Data governed by **Unity Catalog** | Served via **Databricks Dashboards + Genie**

---

*Feel free to ⭐ the repo if you found this useful!*
