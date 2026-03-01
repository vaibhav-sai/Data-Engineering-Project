# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Table in a Dataframe

# COMMAND ----------

df = spark.read.table("ecommerce.bronze.brz_order_items")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Transformations and Cleaning

# COMMAND ----------

df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' → 2 and cast to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df = df.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Transformation : Remove '%' from discount_pct and cast to double
df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Transformation : coupon code processing (convert to lower)
df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Transformation : channel processing 
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

#Transformation : Add processed time 
df = df.withColumn(
    "processed_time", F.current_timestamp()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Silver Table

# COMMAND ----------

# source data
source_df = df  # your DataFrame

# Expose it as a SQL temp view
source_df.createOrReplaceTempView("src_order_items_v")

# COMMAND ----------

spark.sql("USE CATALOG ecommerce")     # <- adjust if you use Unity Catalog
spark.sql("USE SCHEMA silver")         # <- adjust schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.silver.silver_order_items
# MAGIC AS SELECT * FROM src_order_items_v
# MAGIC LIMIT 0;   -- create empty table with the same schema

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ecommerce.silver.silver_order_items AS t
# MAGIC USING src_order_items_v AS s
# MAGIC ON  t.order_id = s.order_id
# MAGIC AND t.item_seq = s.item_seq            -- composite key
# MAGIC AND t.dt       = s.dt                  -- include partition column to prune work
# MAGIC
# MAGIC WHEN MATCHED AND NOT (
# MAGIC     t.order_ts            <=> s.order_ts            AND
# MAGIC     t.customer_id         <=> s.customer_id         AND
# MAGIC     t.product_id          <=> s.product_id          AND
# MAGIC     t.quantity            <=> s.quantity            AND
# MAGIC     t.unit_price_currency <=> s.unit_price_currency AND
# MAGIC     t.unit_price          <=> s.unit_price          AND
# MAGIC     t.discount_pct        <=> s.discount_pct        AND
# MAGIC     t.tax_amount          <=> s.tax_amount          AND
# MAGIC     t.channel             <=> s.channel             AND
# MAGIC     t.coupon_code         <=> s.coupon_code         AND
# MAGIC     t._rescued_data       <=> s._rescued_data       AND
# MAGIC     t.ingest_timestamp    <=> s.ingest_timestamp    AND
# MAGIC     t.source_file         <=> s.source_file         AND
# MAGIC     t.processed_time      <=> s.processed_time
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     t.order_ts            = s.order_ts,
# MAGIC     t.customer_id         = s.customer_id,
# MAGIC     t.product_id          = s.product_id,
# MAGIC     t.quantity            = s.quantity,
# MAGIC     t.unit_price_currency = s.unit_price_currency,
# MAGIC     t.unit_price          = s.unit_price,
# MAGIC     t.discount_pct        = s.discount_pct,
# MAGIC     t.tax_amount          = s.tax_amount,
# MAGIC     t.channel             = s.channel,
# MAGIC     t.coupon_code         = s.coupon_code,
# MAGIC     t._rescued_data       = s._rescued_data,
# MAGIC     t.ingest_timestamp    = s.ingest_timestamp,
# MAGIC     t.source_file         = s.source_file,
# MAGIC     t.processed_time      = s.processed_time
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     dt,
# MAGIC     order_ts,
# MAGIC     customer_id,
# MAGIC     order_id,
# MAGIC     item_seq,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     unit_price_currency,
# MAGIC     unit_price,
# MAGIC     discount_pct,
# MAGIC     tax_amount,
# MAGIC     channel,
# MAGIC     coupon_code,
# MAGIC     _rescued_data,
# MAGIC     ingest_timestamp,
# MAGIC     source_file,
# MAGIC     processed_time
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.dt,
# MAGIC     s.order_ts,
# MAGIC     s.customer_id,
# MAGIC     s.order_id,
# MAGIC     s.item_seq,
# MAGIC     s.product_id,
# MAGIC     s.quantity,
# MAGIC     s.unit_price_currency,
# MAGIC     s.unit_price,
# MAGIC     s.discount_pct,
# MAGIC     s.tax_amount,
# MAGIC     s.channel,
# MAGIC     s.coupon_code,
# MAGIC     s._rescued_data,
# MAGIC     s.ingest_timestamp,
# MAGIC     s.source_file,
# MAGIC     s.processed_time
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_order_items
# MAGIC limit 10;

# COMMAND ----------

