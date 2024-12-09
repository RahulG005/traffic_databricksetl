# Databricks notebook source
# MAGIC %md
# MAGIC ##Extracting Checkpoint , Bronze , silver container URLs

# COMMAND ----------

# MAGIC %run "/Workspace/development/common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading the data from Bronze table

# COMMAND ----------


def read_BronzeTrafficTable(environment):
    print('Reading the Bronze Table Data : ',end='')
    df_bronzeTraffic = (spark.readStream
                    .table(f"`{environment}_catalog`.`bronze`.raw_traffic")
                    )
    print(f'Reading {environment}_catalog.bronze.raw_traffic Success!')
    return df_bronzeTraffic

# COMMAND ----------

# MAGIC %md
# MAGIC ##handling duplicate rows

# COMMAND ----------

def remove_Dups(df):
    print('Removing Duplicate values: ', end='')
    df_dup = df.dropDuplicates()
    print('Success!! ')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ##handling Null values by replacing them

# COMMAND ----------

def handle_NULLs(df,columns):
    print('Replacing NULL values on String Columns with "Unknown" ' , end='')
    df_string = df.fillna('Unknown',subset= columns)
    print('Successs!! ')

    print('Replacing NULL values on Numeric Columns with "0" ' , end='')
    df_clean = df_string.fillna(0,subset = columns)
    print('Success!! ')

    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ##getting count of electric vehicle by creating new column

# COMMAND ----------

def ev_Count(df):
    print('Creating Electric Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicles_Count',
                            col('EV_Car') + col('EV_Bike')
                            )
    
    print('Success!! ')
    return df_ev

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating columns to get  count of all motor vehicles

# COMMAND ----------

def Motor_Count(df):
    print('Creating All Motor Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_motor = df.withColumn('Motor_Vehicles_Count',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')
                            )
    
    print('Success!! ')
    return df_motor

# COMMAND ----------

# MAGIC %md
# MAGIC ##creating transformed time column

# COMMAND ----------

def create_TransformedTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Transformed Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time',
                      current_timestamp()
                      )
    print('Success!!')
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##writing the transformed data to silver_traffic_table

# COMMAND ----------

def write_Traffic_SilverTable(StreamingDF,environment):
    print('Writing the silver_traffic Data : ',end='') 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_traffic` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC ##calling all functions

# COMMAND ----------

## Reading the bronze traffic data

df_trafficdata = read_BronzeTrafficTable(env)

# To remove duplicate rows

df_dups = remove_Dups(df_trafficdata)

# To raplce any NULL values
Allcolumns =df_dups.schema.names
df_nulls = handle_NULLs(df_dups,Allcolumns)

## To get the total EV_Count

df_ev = ev_Count(df_nulls)


## To get the Total Motor vehicle count

df_motor = Motor_Count(df_ev)

## Calling Transformed time function

df_final = create_TransformedTime(df_motor)

## Writing to silver_traffic

write_Traffic_SilverTable(df_final, env)
