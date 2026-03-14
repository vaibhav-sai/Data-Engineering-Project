# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/vaibhav.thirumalesh@gmail.com/consolidated_pipeline/01_Setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

dbutils.widgets.text('catalog','fmcg','Catalog')
dbutils.widgets.text('data_source','customers','Data Source')

# COMMAND ----------


catalog = dbutils.widgets.get('catalog')
data_source = dbutils.widgets.get('data_source')

print(catalog,data_source)

# COMMAND ----------

base_path = f's3://sportsbardb-vaibhav/full_load/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format('csv')
        .option('header',True)
        .option('inferSchema',True)
        .load(base_path)
        .withColumn('read_timestamp',F.current_timestamp())
        .select('*',"_metadata.file_name","_metadata.file_size","_metadata.file_path")
)
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f'select * from {catalog}.{bronze_schema}.{data_source};')
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Drop Duplicates

# COMMAND ----------

df_duplicates = df_bronze.groupBy('customer_id').count().filter(F.col('count')>1)
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['customer_id'])

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Trim spaces in customer name

# COMMAND ----------

display(
    df_silver.filter(F.col('customer_name') != F.trim(F.col('customer_name')))
)

# COMMAND ----------

df_silver =  df_silver.withColumn('customer_name',F.trim(F.col('customer_name')))

# COMMAND ----------

display(
    df_silver.filter(F.col('customer_name') != F.trim(F.col('customer_name')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Data Quality Fix: Correcting City Typos

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

df_silver = df_silver.replace(city_mapping,subset=['city'])

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]
df_silver = df_silver.withColumn(
    "city",
    F.when(F.col("city").isin(allowed), F.col("city")).otherwise(F.lit(None))
)


# COMMAND ----------

# MAGIC %md
# MAGIC - 4: Fix Title-Casing Issue

# COMMAND ----------

df_silver = df_silver.withColumn(
  "customer_name",
  F.when(F.col('customer_name').isNull(),None).otherwise(F.initcap('customer_name'))
)


# COMMAND ----------

# MAGIC %md
# MAGIC - 5: Handling missing cities

# COMMAND ----------

df_silver.filter(F.col('city').isNull()).show()

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col('customer_name').isin(null_customer_names) & F.col('city').isNull()).show()

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

# COMMAND ----------

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = df_silver.join(df_fix,'customer_id','left')
display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn(
    'city',
    F.coalesce('city','fixed_city') # Replace null with fixed city
)

# COMMAND ----------

df_silver=df_silver.drop('fixed_city')

# COMMAND ----------

# Sanity Checks

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names) & F.col('city').isNull()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - 6: Convert customer_id to string

# COMMAND ----------

df_silver = df_silver.withColumn('customer_id',F.col('customer_id').cast('string'))
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

df_silver = (
  df_silver
  # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
  .withColumn('customer',F.concat_ws('-','customer_name',F.coalesce(F.col('city'),F.lit('Unknown'))))
  # Static attributes aligned with parent data model
  .withColumn('market',F.lit('India'))
  .withColumn('platform',F.lit('Sports Bar'))
  .withColumn('channel',F.lit('Acquisition'))
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

delta_table = DeltaTable.forName(spark,'fmcg.gold.dim_customers')
df_child_customers = spark.table('fmcg.gold.sb_dim_customers').select(
  F.col('customer_id').alias('customer_code'),
  'customer',
  'market',
  'platform',
  'channel'
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

