# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze to Silver: Data Cleaning and Transformation for Dimension **Tables**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(5)

# COMMAND ----------

df_silver_brand=df_bronze.withColumn('brand_name',F.trim(F.col('brand_name')))
df_silver_brand.show(5)

# COMMAND ----------

df_silver_brand = df_silver_brand.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
df_silver_brand.show(10)

# COMMAND ----------

df_silver_brand.select('category_code').distinct().show()

# COMMAND ----------

# Anomalies dictionary
anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

# PySpark replace

df_silver_brand = df_silver_brand.replace(to_replace=anomalies,subset=['category_code'])

# ✅ Show results
df_silver_brand.select("category_code").distinct().show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)
df_silver_brand.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Category**

# COMMAND ----------

df_silver_category = spark.table(f"{catalog_name}.bronze.brz_category")

df_silver_category.show(10)

# COMMAND ----------

df_duplicates = df_silver_category.groupBy("category_code").count().filter("count > 1")
df_duplicates.show()


# COMMAND ----------

df_silver_category = df_silver_category.dropDuplicates(["category_code"])
df_silver_category.show(10)

# COMMAND ----------

df_silver_category = df_silver_category.withColumn('category_code',F.upper(F.col("category_code")))
display(df_silver_category.limit(10))

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver_category.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Products**

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_silver_products = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_silver_products.count(), len(df_silver_products.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

# COMMAND ----------

display(df_silver_products.limit(10))

# COMMAND ----------

df_silver_products.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# replace 'g' with ''
df_silver_products = df_silver_products.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver_products.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# replace , with .
df_silver_products = df_silver_products.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver_products.select("length_cm").show(3)

# COMMAND ----------

# convert category_code and brand_code to upper case
df_silver_products = df_silver_products.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
).withColumn(
    "brand_code",
    F.upper(F.col("brand_code"))
)
df_silver_products.select("category_code", "brand_code").show(2)

# COMMAND ----------

# Fix spelling mistakes
df_silver_products = df_silver_products.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver_products.select("material").distinct().show()

# COMMAND ----------

# Convert negative rating_count to positive
df_silver_products = df_silver_products.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)

# COMMAND ----------

# Check final cleaned data

df_silver_products.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver_products.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Customers**

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_silver_customer = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_silver_customer.count(), len(df_silver_customer.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_silver_customer.show(10)

# COMMAND ----------

null_count = df_silver_customer.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

# There are 300 null values in customer_id column. Display some of those
df_silver_customer.filter(F.col("customer_id").isNull()).show(3)

# COMMAND ----------

# Drop rows where 'customer_id' is null
df_silver_customer = df_silver_customer.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver_customer.count()
print(f"Row count after droping null values: {row_count}")

# COMMAND ----------

null_count = df_silver_customer.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 

# COMMAND ----------

df_silver_customer.filter(F.col("phone").isNull()).show(3)

# COMMAND ----------

### Fill null values with 'Not Available'
df_silver_customer = df_silver_customer.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver_customer.filter(F.col("phone").isNull()).show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver_customer.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar/**Date**

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_silver_date = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_silver_date.count(), len(df_silver_date.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_silver_date.show(3)

# COMMAND ----------

# Find duplicate rows in the DataFrame
duplicates = df_silver_date.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# COMMAND ----------

# Remove duplicate rows
df_silver_date = df_silver_date.dropDuplicates(['date'])

# Get row count
row_count = df_silver_date.count()

print("Rows After removing Duplicates: ", row_count)

# COMMAND ----------

# Capitalize first letter of each word in day_name
df_silver_date = df_silver_date.withColumn("day_name", F.initcap(F.col("day_name")))

df_silver_date.show(5)

# COMMAND ----------

df_silver_date = df_silver_date.withColumn("week_of_year", F.abs(F.col("week_of_year")))  # Convert negative to positive

df_silver_date.show(3)

# COMMAND ----------

df_silver_date = df_silver_date.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver_date = df_silver_date.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver_date.show(3)

# COMMAND ----------

# Rename a column
df_silver_date = df_silver_date.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver_date.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

