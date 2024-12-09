# Databricks notebook source
# MAGIC %run "/Workspace/development/common"

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

def create_schema(environment,variable):
    print(f'Using {environment}_Catalog ')
    spark.sql(f"""USE CATALOG {environment}_catalog""")
    print(f'Creating {variable} Schema in {environment}_Catalog')
    path = spark.sql(f"""describe external location {variable} """).select("url").collect()[0][0]
    spark.sql(f"""create schema if not exists {variable} managed location '{path}/{variable}'""")
    print("************************************")


# COMMAND ----------

# MAGIC %md
# MAGIC ##creating bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###creating raw_traffic table

# COMMAND ----------

def createTable_rawTraffic(environment):
    print(f'Creating raw_Traffic table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_traffic`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating raw_roads table

# COMMAND ----------

def createTable_rawRoad(environment):
    print(f'Creating raw_roads table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}_catalog`.`bronze`.`raw_roads`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
                    );""")
    
    print("************************************")

# COMMAND ----------

create_schema(env,"bronze")
createTable_rawTraffic(env)
createTable_rawRoad(env)



create_schema(env,"silver")
create_schema(env,"gold")
