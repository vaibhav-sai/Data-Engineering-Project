# 🛒 ShopVista E-Commerce Data Pipeline — Azure Databricks Medallion Architecture

> **End-to-end ELT pipeline on Azure Databricks using the Medallion (Bronze → Silver → Gold) architecture to transform raw e-commerce CSV data into BI-ready analytics tables, powering a Power BI dashboard with $22.19B in tracked sales across 7 countries.**

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
  - [Gold Layer — BI-Ready Aggregations](#gold-layer--bi-ready-aggregations)
- [Data Model](#data-model)
- [Analytics Dashboard](#analytics-dashboard)
- [Key Metrics](#key-metrics)
- [Setup & Usage](#setup--usage)
- [Learnings & Highlights](#learnings--highlights)

---

## Project Overview

**ShopVista** is a fictional global e-commerce brand operating across 7 countries (India, USA, UK, Australia, UAE, Singapore, Canada). This project builds a production-grade data engineering pipeline that:

1. **Ingests** raw CSV files from Azure Data Lake Storage (ADLS Gen2) into Databricks
2. **Transforms** data through three quality layers — Bronze, Silver, and Gold
3. **Serves** cleaned, enriched, and aggregated data to a Power BI dashboard for business intelligence

The pipeline processes **dimension tables** (products, customers, brands, categories, calendar) and **fact tables** (order items, daily summaries) using Delta Lake for ACID transactions and schema enforcement.

---

## Architecture

```
ShopVista System
      │
      │  CSV files
      ▼
Azure Data Lake Storage (ADLS Gen2)
      │
      │  Access Connector (Secure Access)
      ▼
Azure Databricks (ETL/ELT)
  ┌─────────────────────────────────────┐
  │  Unity Catalog — "ecommerce"         │
  │                                     │
  │  Bronze Schema  →  Silver Schema  → Gold Schema  │
  └─────────────────────────────────────┘
      │
      ▼
Power BI — Analytics & Reporting Layer
```

> See `project_architecture.png` for the full visual diagram.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute & Processing | Azure Databricks |
| Data Format | Delta Lake (ACID, Time Travel) |
| Catalog & Governance | Unity Catalog |
| Language | Python (PySpark), SQL |
| Ingestion | Auto Loader (CloudFiles) for streaming fact data |
| BI & Reporting | Power BI |

---

## Project Structure

```
📦 ecommerce-databricks-pipeline
 ┣ 📄 Catalog___Schema_Creation.py   # Step 0 — Unity Catalog setup
 ┣ 📄 1_dim_bronze.py                # Dimension tables: Bronze ingestion
 ┣ 📄 2_dim_Silver.py                # Dimension tables: Silver cleansing
 ┣ 📄 3_dim_gold.py                  # Dimension tables: Gold enrichment
 ┣ 📄 1_fact_bronze.py               # Fact table: Bronze streaming ingestion
 ┣ 📄 2_fact_silver.py               # Fact table: Silver MERGE upsert
 ┣ 📄 3_fact_gold.py                 # Fact table: Gold KPI MERGE upsert
 ┣ 📄 4_daily_summary.py             # Gold daily summary (incremental refresh)
 ┣ 🖼️ project_architecture.png       # Pipeline architecture diagram
 ┗ 🖼️ ecommerce_analytics_report.jpg # Power BI dashboard screenshot
```

---

## Pipeline Walkthrough

### Catalog & Schema Setup

**File:** `Catalog___Schema_Creation.py`

Creates the Unity Catalog and all three schemas:

```sql
CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;
```

---

### Bronze Layer — Raw Ingestion

Raw CSV data is loaded from ADLS volumes into Delta tables with minimal transformation. Metadata columns (`_source_file`, `ingest_timestamp`) are added to every table for lineage tracking.

**Dimension tables ingested:**

| Source File | Bronze Table |
|---|---|
| brands.csv | `brz_brands` |
| category.csv | `brz_category` |
| products.csv | `brz_products` |
| customers.csv | `brz_customers` |
| date.csv | `brz_calendar` |

**Fact table (streaming):** The order items fact table uses **Auto Loader** (`cloudFiles`) for incremental streaming ingestion, processing only new CSV files from the landing zone:

```python
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  ...
  .writeStream.trigger(availableNow=True)
  .toTable("ecommerce.bronze.brz_order_items")
```

---

### Silver Layer — Data Cleansing & Transformation

Each Bronze table is cleaned, standardised, and validated before writing to Silver using `overwrite` (dimensions) or `MERGE` upserts (facts).

**Key transformations applied:**

- **Brands:** Trimmed whitespace, removed special characters from `brand_code`, corrected anomalous `category_code` values (`GROCERY → GRCY`, `BOOKS → BKS`, `TOYS → TOY`)
- **Category:** Removed duplicates on `category_code`, standardised to `UPPER CASE`
- **Products:** Stripped unit suffixes from `weight_grams` (e.g., `"250g" → 250`), fixed decimal separator in `length_cm` (`","` → `"."`), corrected material spelling mistakes (`Coton → Cotton`, `Alumium → Aluminum`, `Ruber → Rubber`), converted negative `rating_count` to absolute values
- **Customers:** Dropped 300 rows with null `customer_id`; filled null `phone` values with `"Not Available"`
- **Calendar:** Removed duplicate dates, standardised `day_name` casing (`initcap`), converted negative week numbers to positive, formatted `quarter` as `"Q1-2024"` and `week` as `"Week12-2024"`
- **Order Items (Fact):** Deduplicated on `(order_id, item_seq)`, converted text quantity (`"Two" → 2`), stripped `$` from `unit_price`, stripped `%` from `discount_pct`, normalised channel values (`web → Website`, `app → Mobile`), applied a `MERGE INTO` upsert pattern for idempotent loads

---

### Gold Layer — BI-Ready Aggregations

The Gold layer assembles the final BI-ready tables through joins, enrichment, and KPI calculations.

**Dimension tables:**

| Gold Table | Description |
|---|---|
| `gld_dim_products` | Products enriched with brand name and category name via CTE join |
| `gld_dim_customers` | Customers enriched with geographic `region` column (e.g., South, West, NorthEast) derived from a hand-crafted country→state→region mapping dictionary |
| `gld_dim_date` | Calendar with `date_id` (integer `yyyyMMdd`), `month_name`, and `is_weekend` flag |

**Fact tables:**

| Gold Table | Description |
|---|---|
| `gld_fact_order_items` | Transactional order items with derived KPIs: `gross_amount`, `discount_amount`, `net_amount`, `coupon_flag`, `date_id`; loaded via `MERGE INTO` |
| `gld_fact_daily_orders_summary` | Daily aggregated summary (quantity, gross, discount, tax, net) by `date_id` and `currency`; uses a 30-day rolling window for incremental refresh |

**KPI derivations in Gold:**

```python
gross_amount   = quantity × unit_price
discount_amount = CEIL(gross_amount × discount_pct / 100)
net_amount      = gross_amount − discount_amount + tax_amount
coupon_flag     = 1 if coupon_code IS NOT NULL else 0
date_id         = date formatted as yyyyMMdd (INTEGER)
```

---

## Data Model

```
gld_dim_date ──────────────────────────────┐
gld_dim_customers ─────────────────────────┤
gld_dim_products ──────────────────────────┤
                                           │
                               gld_fact_order_items
                                           │
                               gld_fact_daily_orders_summary
```

The Gold layer follows a **Star Schema** design, with `gld_fact_order_items` at the centre linked to date, customer, and product dimension tables via surrogate/natural keys.

---

## Analytics Dashboard

The Power BI dashboard connects directly to the Gold layer and surfaces:

| KPI | Value |
|---|---|
| Total Sales | **$22.19 Billion** |
| Units Sold | **1.6 Million** |
| Total Customers | **299.7K** |
| Repeat Customer Rate | **66.53%** |
| Average Discount % | **8.67%** |
| High-Profit Region | **South** |

**Visuals included:**
- 📊 Customer Count by Region (horizontal bar chart)
- 🍩 Total Sales by Channel — Mobile (45%) vs Website (55%)
- 📈 Revenue Trend by Month (Jan 2024 – Jul 2025)
- 📋 Brand × Category Net Total table
- 🔽 Slicers for Country, Brand, Category, and Date Range

---

## Setup & Usage

### Prerequisites

- Azure Subscription with ADLS Gen2 and Azure Databricks workspace
- Databricks Runtime 13.x+ (with Delta Lake support)
- Unity Catalog enabled on the Databricks workspace
- Power BI Desktop (for dashboard)

### Steps

1. **Create the catalog and schemas** by running `Catalog___Schema_Creation.py` in your Databricks workspace
2. **Upload source CSV files** to the appropriate ADLS volume paths:
   ```
   /Volumes/ecommerce/raw/ecomm_data/brands/
   /Volumes/ecommerce/raw/ecomm_data/category/
   /Volumes/ecommerce/raw/ecomm_data/products/
   /Volumes/ecommerce/raw/ecomm_data/customers/
   /Volumes/ecommerce/raw/ecomm_data/date/
   /Volumes/ecommerce/raw/ecomm_data/order_items/landing/
   ```
3. **Run the notebooks in order:**
   ```
   1_dim_bronze.py  →  2_dim_Silver.py  →  3_dim_gold.py
   1_fact_bronze.py →  2_fact_silver.py →  3_fact_gold.py  →  4_daily_summary.py
   ```
4. **Connect Power BI** to the `ecommerce.gold` schema using the Databricks connector

### Widget Parameters

The fact pipeline notebooks accept a Databricks widget parameter:

```python
dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
```

This allows the catalog name to be overridden at runtime for multi-environment deployments (e.g., `dev`, `staging`, `prod`).

---

## Learnings & Highlights

- ✅ **Auto Loader** for efficient, scalable streaming ingestion of CSV files without manual file tracking
- ✅ **MERGE INTO (Upsert)** pattern for idempotent Silver and Gold loads — safe to re-run without duplicating data
- ✅ **Schema Evolution** (`mergeSchema: true`) to handle source schema changes gracefully
- ✅ **Unity Catalog** for centralised data governance, lineage, and access control
- ✅ **Rescued Data Column** (`_rescued_data`) to capture rows that do not conform to the inferred schema without losing data
- ✅ **Incremental daily summary** with a 30-day rolling window to balance freshness and compute cost
- ✅ **Star Schema** design in Gold optimised for Power BI DirectQuery / Import performance

---

## Author

Built as part of a hands-on Azure Databricks data engineering project.  
Dashboard powered by **Power BI** | Pipeline orchestrated on **Azure Databricks** | Data governed by **Unity Catalog**

---

*Feel free to ⭐ the repo if you found this useful!*
