# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists fmcg;
# MAGIC use catalog fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmcg.gold;
# MAGIC create schema if not exists fmcg.silver;
# MAGIC create schema if not exists fmcg.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables FROM fmcg.gold;

# COMMAND ----------

