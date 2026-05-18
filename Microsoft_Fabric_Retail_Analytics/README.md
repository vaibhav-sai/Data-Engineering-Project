# 🛒 Retail Data Quality & Profitability Insights — Medallion Architecture on Microsoft Fabric

> **End-to-end Data Engineering pipeline using Bronze → Silver → Gold Medallion Architecture in Microsoft Fabric with PySpark, Delta Tables, and Power BI readiness.**

---

## 📌 Short Description

End-to-end retail data pipeline on Microsoft Fabric using Medallion Architecture (Bronze→Silver→Gold). Cleans messy orders, inventory & returns data with PySpark, generates KPIs, and serves Power BI-ready Delta tables.

---

## 📖 Project Description

A retail company needed a unified, clean, and analytics-ready view of its operations — combining messy, inconsistent data from three sources: Orders, Inventory, and Returns. This project implements a full Medallion Architecture (Bronze → Silver → Gold) inside Microsoft Fabric's Lakehouse.

Raw CSV data is ingested into the Bronze layer via a Data Pipeline with Copy Activities. PySpark Notebooks then clean and standardize the data — fixing column names, normalizing dates across multiple formats, converting text-based numbers (e.g. "twenty five" → 25), stripping currency symbols, validating emails, and removing duplicates — producing clean Silver Delta tables.

A Gold aggregation notebook joins all three Silver tables and computes per-product KPIs: Total Orders, Unique Customers, Return Rate, Total Revenue, Average Order Value, Net Profit, and more. The final `gold_product_month_kpis` table is exposed via SQL Analytics Endpoint and is Power BI-ready.

---

## 🏗️ Architecture

```
Raw Source Files (CSV)
        │
        ▼
┌─────────────────────┐
│   Data Pipeline     │  ← Copy Activity (Orders, Inventory, Returns)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   🥉 Bronze Layer   │  ← Raw Delta Tables (no transformation)
│  bronze_orders      │
│  bronze_inventory   │
│  bronze_returns     │
└─────────────────────┘
        │
        ▼ PySpark Notebooks
┌─────────────────────┐
│   🥈 Silver Layer   │  ← Cleaned & Standardized Delta Tables
│  silver_orders      │
│  silver_inventory   │
│  silver_returns     │
└─────────────────────┘
        │
        ▼ Join + Aggregation Notebook
┌─────────────────────────────┐
│        🥇 Gold Layer        │  ← Business KPIs Delta Table
│  gold_product_month_kpis    │
└─────────────────────────────┘
        │
        ▼
┌────────────────────────────────────┐
│  SQL Analytics Endpoint + Power BI │
└────────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Component | Technology |
|---|---|
| Platform | Microsoft Fabric |
| Storage | OneLake / Lakehouse |
| Table Format | Delta Lake |
| Processing | PySpark (Notebooks) |
| Orchestration | Data Pipeline (Copy Activity) |
| Query Layer | SQL Analytics Endpoint (T-SQL) |
| Visualization | Power BI |
| Schema | `Retail_Analytics` |

---

## 📂 Project Structure

```
Retail Lakehouse/
│
├── Files/
│   └── Bronze Data (Raw Data)/
│       ├── Orders Data.Parquet
│       ├── Inventory Data.Parquet
│       └── Returns Data.Parquet
│
├── Tables/
│   └── Retail_Analytics/
│       ├── bronze_orders
│       ├── bronze_inventory
│       ├── bronze_returns
│       ├── silver_orders
│       ├── silver_inventory
│       ├── silver_returns
│       └── gold_product_month_kpis
│
└── Notebooks/
    ├── Bronze_to_Silver_Orders.ipynb
    ├── Bronze_to_Silver_Inventory.ipynb
    ├── Bronze_to_Silver_Returns.ipynb
    └── Silver_to_Gold_Aggregation.ipynb
```

---

## 🔄 Layer Details

### 🥉 Bronze Layer — Raw Ingestion

- Data ingested **as-is** from source CSV files via **Pipeline Copy Activities**
- Stored as **Delta tables** under `Retail_Analytics` schema
- No transformations applied — preserves source fidelity

Tables created:
- `Retail_Analytics.Bronze_Orders`
- `Retail_Analytics.Bronze_Inventory`
- `Retail_Analytics.Bronze_Returns`

---

### 🥈 Silver Layer — Data Cleaning & Standardization

#### Orders Cleaning
| Issue | Fix Applied |
|---|---|
| Inconsistent column names | Renamed: `Order_ID` → `OrderID`, `cust_id` → `CustomerID`, etc. |
| Mixed date formats | `coalesce(to_date(...))` across 7 format patterns |
| Currency symbols in amounts | `regexp_replace` strips `$`, `₹`, `Rs`, `USD`, `INR` → cast to `DoubleType` |
| Text quantities | `one`/`two`/`three` → `1`/`2`/`3` via `when()` |
| Payment mode variants | `lower(regexp_replace(...))` → normalized (e.g. `Credit-Card` → `creditcard`) |
| Delivery status casing | Lowercased and special chars removed |
| Invalid emails | Regex validation; invalids set to `NULL` |
| Dirty shipping addresses | Stripped `#`, `@`, `!`, `$` |
| Duplicate orders | `dropDuplicates(["OrderID"])` |
| Nulls | `fillna()` — Quantity=0, DeliveryStatus/PaymentMode="unknown" |

#### Inventory Cleaning
| Issue | Fix Applied |
|---|---|
| Text-based stock values | `"twenty five"` → `25`, `"eighteen"` → `18`, etc. |
| Multiple date formats | `regexp_replace([./], -)` + `to_date()` |
| Currency in CostPrice | `regexp_extract` numeric part → `DoubleType` |
| Dirty warehouse names | `trim + initcap + regexp_replace` |
| Availability flags | `yes/no/true/false` → Boolean |

#### Returns Cleaning
| Issue | Fix Applied |
|---|---|
| Column name inconsistencies | Standardized (e.g. `Return_ID` → `ReturnID`) |
| Mixed date formats | Normalized to `dd-MM-yyyy` |
| RefundStatus variants | Lowercased, special chars stripped |
| Currency in ReturnAmount | Numeric extraction → `DoubleType` |
| PickupAddress noise | `initcap + trim + regexp_replace` |

---

### 🥇 Gold Layer — Business KPIs

Joins `silver_orders` ← (LEFT) → `silver_returns` ← (LEFT) → `silver_inventory` on `OrderID` and `ProductName`.

Aggregated per **ProductName**:

| KPI | Formula |
|---|---|
| `Total_Orders` | `count(OrderID)` |
| `Unique_Customers` | `countDistinct(CustomerID)` |
| `Total_Returns` | `count(ReturnID)` |
| `Return_Rate_%` | `(Total_Returns / Total_Orders) * 100` |
| `Total_Revenue` | `sum(OrderAmount)` |
| `Avg_Order_Value` | `avg(OrderAmount)` |
| `Total_Stock` | `sum(Stock)` |
| `Avg_Cost` | `avg(CostPrice)` |
| `Net_Profit` | `sum(OrderAmount) − (sum(Stock) × avg(CostPrice))` |

Saved as: `Retail_Analytics.gold_product_month_kpis` (Delta, overwrite mode)

---

## 🗄️ SQL Analytics — Sample Queries

```sql
-- View full KPI table
SELECT * FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]

-- Total Orders
SELECT SUM(Total_Orders) AS total_orders
FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]

-- Total Revenue
SELECT SUM(Total_Revenue) AS total_revenue
FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]

-- Total Returns
SELECT SUM(Total_Returns) AS total_returns
FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]

-- Average Cost
SELECT SUM(Avg_Cost) AS Avg_Cost
FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]

-- Total Stock
SELECT SUM(Total_Stock) AS Total_Stock
FROM [Retail_Lakehouse].[Retail_Analytics].[gold_product_month_kpis]
```

---

## ✅ Key Concepts Demonstrated

- ✔ **Medallion Architecture** (Bronze / Silver / Gold)
- ✔ **Microsoft Fabric Lakehouse & OneLake**
- ✔ **Data Pipeline with Copy Activity**
- ✔ **PySpark Data Cleaning** (regex, type casting, null handling, deduplication)
- ✔ **Delta Table** read/write with `saveAsTable()`
- ✔ **Multi-table Joins** with alias disambiguation
- ✔ **KPI Aggregation** using `groupBy` + `agg`
- ✔ **SQL Analytics Endpoint** (T-SQL over Fabric tables)
- ✔ **Power BI Readiness** via Gold Delta table

---

## 🚀 How to Reproduce

1. **Create a Microsoft Fabric Workspace** and add a Lakehouse named `Retail Lakehouse`
2. **Create schema** `Retail_Analytics` inside the Lakehouse
3. **Upload raw data files** (Orders, Inventory, Returns) as Parquet under `Files/Bronze Data (Raw Data)/`
4. **Create and run the Data Pipeline** with three Copy Activities (one per source) targeting Bronze Delta tables
5. **Run the Silver Notebooks** in order: Orders → Inventory → Returns
6. **Run the Gold Aggregation Notebook** to generate `gold_product_month_kpis`
7. **Query via SQL Endpoint** or connect Power BI to the Gold table

---

## 📊 Output

The final `gold_product_month_kpis` Delta table is ready for:
- **Power BI dashboards** — revenue trends, return analysis, profitability by product
- **SQL ad-hoc queries** — via Fabric's SQL Analytics Endpoint
- **Semantic model** integration for self-service BI

---

## 👤 Author

Built as a hands-on Microsoft Fabric Data Engineering project demonstrating real-world retail data quality and analytics patterns using the Medallion Architecture.

---

## 📄 License

This project is for educational and portfolio purposes.
