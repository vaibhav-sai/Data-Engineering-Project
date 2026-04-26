# 🛍️ Shopping Mart Analytics — Microsoft Fabric Medallion Architecture

> **End-to-end data engineering pipeline** built on Microsoft Fabric, implementing the Medallion Architecture to process structured and unstructured retail data into actionable business insights via Power BI.

---

## 📋 Project Short Info

> End-to-end retail data engineering solution on Microsoft Fabric using Medallion Architecture (Bronze → Silver → Gold) with PySpark, Delta Lake, and Power BI for KPI reporting.

---

## 📖 Project Description

This project delivers a complete data engineering solution for a retail business (Shopping Mart) using Microsoft Fabric. It implements the Medallion Architecture to progressively refine raw retail data — orders, customers, products, reviews, social media, and web logs — through Bronze, Silver, and Gold layers. PySpark notebooks handle cleaning, enrichment, and aggregation; Delta Lake enables high-performance querying; and Power BI surfaces KPIs via Direct Lake mode with interactive dashboards.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Microsoft Fabric Platform                     │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │    BRONZE    │───▶│    SILVER    │───▶│      GOLD        │  │
│  │  Raw Layer   │    │ Cleaned Layer│    │  Curated Layer   │  │
│  │              │    │              │    │                  │  │
│  │  CSV / JSON  │    │   Parquet    │    │  Delta Tables    │  │
│  └──────────────┘    └──────────────┘    └──────────────────┘  │
│         │                  │                      │             │
│   API Ingestion       PySpark ETL           Power BI Report     │
│   (Pipelines)         (Notebooks)          (Direct Lake Mode)   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```
shopping-mart-fabric/
│
├── notebooks/
│   ├── Bronze_to_Silver_Cleaning.ipynb   # Data cleaning & integration (PySpark)
│   └── Silver_to_Gold.ipynb              # Aggregations & Delta table creation
│
├── pipelines/
│   ├── Structured_Ingestion_Pipeline     # CSV data ingestion via metadata-driven pipeline
│   ├── Unstructured_Ingestion_Pipeline   # JSON data ingestion
│   └── Master_Pipeline                   # Orchestrates all ingestion + transformation steps
│
├── reports/
│   └── ShoppingMart_Analytics.pbix       # Power BI report (Direct Lake mode)
│
└── README.md
```

---

## 🗂️ Data Sources

| Dataset | Format | Layer | Description |
|---|---|---|---|
| Orders | CSV | Bronze | Transaction records |
| Customers | CSV | Bronze | Customer demographics |
| Products | CSV | Bronze | Product catalog & inventory |
| Product Reviews | JSON | Bronze | Unstructured user reviews with ratings |
| Social Media Sentiments | JSON | Bronze | Platform-level sentiment data |
| Web Engagement Logs | JSON | Bronze | User page interaction events |

---

## ⚙️ Tech Stack

| Component | Technology |
|---|---|
| Platform | Microsoft Fabric |
| Storage | OneLake (ADLS Gen2) |
| Processing | Apache Spark (PySpark) |
| Table Format | Delta Lake |
| Orchestration | Fabric Data Pipelines |
| Reporting | Power BI (Direct Lake Mode) |
| File Formats | CSV, JSON → Parquet → Delta |

---

## 🔄 Pipeline Walkthrough

### 1. Bronze Layer — Raw Ingestion

A **metadata-driven pipeline** reads a JSON config file listing all source URLs and destinations. A `Lookup Activity` feeds a `ForEach Activity` that dynamically triggers `Copy Activities` — avoiding hardcoded pipelines per file.

**Data ingested:**
- `ShoppingMart_customers.csv`
- `ShoppingMart_orders.csv`
- `ShoppingMart_products.csv`
- `ShoppingMart_review.json`
- `ShoppingMart_social_media.json`
- `ShoppingMart_web_logs.json`

---

### 2. Silver Layer — Cleaning & Integration

**Notebook:** `Bronze_to_Silver_Cleaning.ipynb`

```python
# Drop nulls in critical columns
df_orders = df_orders.dropna(subset=["OrderID", "CustomerID", "ProductID", "OrderDate", "TotalAmount"])

# Convert string dates to proper date type
df_orders = df_orders.withColumn("OrderDate", to_date(col("OrderDate")))

# Join orders with customers and products for a unified view
df_orders = df_orders \
    .join(df_customers, on='CustomerID', how="inner") \
    .join(df_products, on='ProductID', how="inner")

# Write cleaned data as Parquet to Silver lakehouse
df_orders.write.mode("overwrite").parquet("<silver_path>/ShoppingMart_customers_orderdata")
```

Fabric **Shortcuts** are used to access Bronze files from within the Silver lakehouse — no data duplication.

---

### 3. Gold Layer — Aggregations & KPIs

**Notebook:** `Silver_to_Gold.ipynb`

```python
# Average product rating per product
reviews_df = reviews_df.groupBy("product_id").agg(avg("rating").alias("AvgRating"))

# Social media sentiment trends by platform
social_df = social_df.groupBy("platform", "sentiment").count()

# Web engagement counts per user, page, and action
weblogs_df = weblogs_df.groupBy("user_id", "page", "action").count()

# Persist as Delta tables for SQL querying
reviews_df.write.format("delta").mode("overwrite").saveAsTable("Shopping_Mart_Gold_Schema.ShoppingMart_review")
social_df.write.format("delta").mode("overwrite").saveAsTable("Shopping_Mart_Gold_Schema.ShoppingMart_social_media")
weblogs_df.write.format("delta").mode("overwrite").saveAsTable("Shopping_Mart_Gold_Schema.ShoppingMart_web_logs")
Orders_df.write.format("delta").mode("overwrite").saveAsTable("Shopping_Mart_Gold_Schema.ShoppingMart_customers_orderdata")
```

---

## 📊 Power BI Dashboard

The report connects to Gold Delta tables using **Direct Lake mode** — combining Import speed with real-time data freshness.

### KPIs & Visuals

| Visual | Description |
|---|---|
| 💰 Total Sales Card | Aggregate revenue across all orders |
| 📦 Products Sold Card | Total units sold |
| 🏆 Top 5 Customers | Ranked by total spend |
| 🏆 Top 5 Products | Ranked by sales and average review rating |
| 📈 Sales Trend by Month | Line chart with monthly data labels |
| 🍩 Sales by Category | Distribution across Home Goods, Sports, Fashion, Electronics |
| 💬 Social Sentiment | Pie chart — positive / neutral / negative breakdown |
| 🖱️ Web Engagement Actions | Pie chart — purchase / view / add_to_cart / click |
| 📋 Product Inventory | Stock levels by product, category |

**Interactive Features:**
- Slicers: Year, Month, Day, Product, Category
- Drill-down on visuals
- Custom Date Table (SQL) for time-intelligence functions
- Customized cross-filter interactions for contextual accuracy

---

## 🔁 Orchestration

A **Master Pipeline** orchestrates the full workflow sequentially:

```
Master Pipeline
├── 1. Structured Ingestion Pipeline   (CSV → Bronze)
├── 2. Unstructured Ingestion Pipeline (JSON → Bronze)
├── 3. Bronze → Silver Notebook        (Cleaning & Integration)
└── 4. Silver → Gold Notebook          (Aggregations & Delta Tables)
```

- **Monitoring:** Fabric Medallion Task Flow for visual pipeline health tracking
- **Alerting:** Email notifications configured for pipeline success / failure

---

## 🚀 Getting Started

### Prerequisites

- Microsoft Fabric workspace (with Lakehouse enabled)
- Power BI Desktop (for `.pbix` editing)
- Source data files (CSV + JSON) hosted at an accessible API/URL

### Setup Steps

1. **Create three Lakehouses** in your Fabric workspace: `Bronze`, `Silver`, `Gold`
2. **Upload the metadata JSON** with source file URLs and destination paths
3. **Import and run** the ingestion pipelines (structured + unstructured)
4. **Run** `Bronze_to_Silver_Cleaning.ipynb` to clean and enrich data
5. **Run** `Silver_to_Gold.ipynb` to generate aggregated Delta tables
6. **Connect Power BI** to the Gold lakehouse SQL endpoint via Direct Lake mode
7. **Set up the Master Pipeline** to automate the full flow end-to-end

---

## 📸 Dashboard Preview

<img width="1920" height="1080" alt="Screenshot (404)" src="https://github.com/user-attachments/assets/3bab723d-82ad-4151-8f29-7026d9f1e641" />


---

## 🙋 Author

**Vaibhav Tutika**  
https://www.linkedin.com/in/vaibhav-thirumalesh/?skipRedirect=true

---

## 📄 License

This project is for educational and portfolio purposes.
