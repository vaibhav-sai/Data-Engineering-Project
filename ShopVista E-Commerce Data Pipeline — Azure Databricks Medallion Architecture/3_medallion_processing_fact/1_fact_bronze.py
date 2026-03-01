# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# -------------------------
# -------------------------

adls_path = f"/Volumes/ecommerce/raw/ecomm_data/order_items/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"/Volumes/ecommerce/raw/ecomm_data/checkpoint/bronze/fact_order_items/"


# COMMAND ----------

spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_items") \
 .awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT max(dt) FROM {catalog_name}.bronze.brz_order_items"))
display(spark.sql(f"SELECT min(dt) FROM {catalog_name}.bronze.brz_order_items"))

# COMMAND ----------

display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('/Volumes/ecommerce/raw/ecomm_data/checkpoint/bronze/fact_order_items/')"
    )
)

# COMMAND ----------

