# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_traffic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_roads

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_traffic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_roads

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_traffic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_roads
# MAGIC
