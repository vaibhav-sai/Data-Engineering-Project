# Databricks notebook source
# MAGIC %md
# MAGIC ### From Silver To Gold: Aggregation and KPI Tables

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

print(catalog_name)

# COMMAND ----------

df = spark.read.table("ecommerce.silver.silver_order_items")
display(df.limit(20))

# COMMAND ----------

df = df.withColumn(
    "gross_amount",
    F.col("quantity") * F.col("unit_price")
    )

# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df = df.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

# 3) Add sale_amount = gross - discount
df = df.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

# add date id
df = df.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df = df.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)

df.limit(5).display()

# COMMAND ----------

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

# source data
source_df = orders_gold_df  # your DataFrame

# Expose it as a SQL temp view
source_df.createOrReplaceTempView("src_gold_order_items_v")

# COMMAND ----------

spark.sql("USE CATALOG ecommerce")     # <- adjust if you use Unity Catalog
spark.sql("USE SCHEMA gold")         # <- adjust schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.gold.gld_fact_order_items
# MAGIC AS SELECT * FROM src_gold_order_items_v
# MAGIC LIMIT 0;   -- create empty table with the same schema

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ecommerce.gold.gld_fact_order_items AS t
# MAGIC USING src_gold_order_items_v AS s
# MAGIC ON  t.transaction_id = s.transaction_id
# MAGIC AND t.seq_no         = s.seq_no
# MAGIC AND t.date_id        = s.date_id         -- include partition column for pruning
# MAGIC
# MAGIC WHEN MATCHED AND NOT (
# MAGIC     t.transaction_date     <=> s.transaction_date     AND
# MAGIC     t.transaction_ts       <=> s.transaction_ts       AND
# MAGIC     t.customer_id          <=> s.customer_id          AND
# MAGIC     t.product_id           <=> s.product_id           AND
# MAGIC     t.channel              <=> s.channel              AND
# MAGIC     t.coupon_code          <=> s.coupon_code          AND
# MAGIC     t.coupon_flag          <=> s.coupon_flag          AND
# MAGIC     t.unit_price_currency  <=> s.unit_price_currency  AND
# MAGIC     t.quantity             <=> s.quantity             AND
# MAGIC     t.unit_price           <=> s.unit_price           AND
# MAGIC     t.gross_amount         <=> s.gross_amount         AND
# MAGIC     t.discount_percent     <=> s.discount_percent     AND
# MAGIC     t.discount_amount      <=> s.discount_amount      AND
# MAGIC     t.tax_amount           <=> s.tax_amount           AND
# MAGIC     t.net_amount           <=> s.net_amount
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     t.transaction_date     = s.transaction_date,
# MAGIC     t.transaction_ts       = s.transaction_ts,
# MAGIC     t.customer_id          = s.customer_id,
# MAGIC     t.product_id           = s.product_id,
# MAGIC     t.channel              = s.channel,
# MAGIC     t.coupon_code          = s.coupon_code,
# MAGIC     t.coupon_flag          = s.coupon_flag,
# MAGIC     t.unit_price_currency  = s.unit_price_currency,
# MAGIC     t.quantity             = s.quantity,
# MAGIC     t.unit_price           = s.unit_price,
# MAGIC     t.gross_amount         = s.gross_amount,
# MAGIC     t.discount_percent     = s.discount_percent,
# MAGIC     t.discount_amount      = s.discount_amount,
# MAGIC     t.tax_amount           = s.tax_amount,
# MAGIC     t.net_amount           = s.net_amount
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     date_id,
# MAGIC     transaction_date,
# MAGIC     transaction_ts,
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     seq_no,
# MAGIC     product_id,
# MAGIC     channel,
# MAGIC     coupon_code,
# MAGIC     coupon_flag,
# MAGIC     unit_price_currency,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     gross_amount,
# MAGIC     discount_percent,
# MAGIC     discount_amount,
# MAGIC     tax_amount,
# MAGIC     net_amount
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.date_id,
# MAGIC     s.transaction_date,
# MAGIC     s.transaction_ts,
# MAGIC     s.transaction_id,
# MAGIC     s.customer_id,
# MAGIC     s.seq_no,
# MAGIC     s.product_id,
# MAGIC     s.channel,
# MAGIC     s.coupon_code,
# MAGIC     s.coupon_flag,
# MAGIC     s.unit_price_currency,
# MAGIC     s.quantity,
# MAGIC     s.unit_price,
# MAGIC     s.gross_amount,
# MAGIC     s.discount_percent,
# MAGIC     s.discount_amount,
# MAGIC     s.tax_amount,
# MAGIC     s.net_amount
# MAGIC   );

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

spark.sql(f"SELECT max(transaction_date) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

