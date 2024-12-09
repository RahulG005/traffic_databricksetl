# Databricks notebook source
# MAGIC %md
# MAGIC #To reuse common functions and variables

# COMMAND ----------

# MAGIC %md
# MAGIC ##Defining all common variables

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold = spark.sql("describe external location `gold`").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Defining common functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing duplicates

# COMMAND ----------

def remove_Dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling nulls

# COMMAND ----------

def handle_NULLs(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')
    return df_numeric
