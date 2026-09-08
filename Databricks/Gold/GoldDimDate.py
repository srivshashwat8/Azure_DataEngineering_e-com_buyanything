# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a widget for incremental run

# COMMAND ----------

dbutils.widgets.text("incremental flag", "0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check incremental run

# COMMAND ----------

incremental_flag = dbutils.widgets.get("incremental flag")
incremental_flag

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish Connection with ADLS

# COMMAND ----------

spark.conf.set(   "fs.azure.account.key.cfstorageadlsgen2.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_KEY>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Connection

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working on Date Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct(OrderDate)
# MAGIC from parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Dataframe- df_src having Dimensions 

# COMMAND ----------

df_src = spark.sql("""
                  
        select distinct(OrderDate)
        from parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`
""")

df_src = df_src.withColumn("Year", F.year(df_src["OrderDate"]))
df_src = df_src.withColumn("Month", F.month(df_src["OrderDate"]))
df_src = df_src.withColumn("Day", F.day(df_src["OrderDate"]))

df_src.display()

# COMMAND ----------

df_src.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Gold layer data - df_sink

# COMMAND ----------

from delta.tables import DeltaTable
path = "abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_date"

if DeltaTable.isDeltaTable(spark, path):
         df_sink =spark.sql(
             """
            select Dim_Date_Key, OrderDate, Year, Month, Day
            from delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_date`
        
             """
            )
         print("Table exists")
else:
        df_sink= spark.createDataFrame([], schema= "Dim_Date_Key int, OrderDate date , Year int, Month int, Day int")
df_sink.display()


    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old and New records - Left Join

# COMMAND ----------

# DBTITLE 1,Old and New records - Left Join
df1= df_src.join(df_sink, df_src["OrderDate"] == df_sink["OrderDate"], "left").select(df_src["OrderDate"], df_src["Year"], df_src["Month"], df_src["Day"], df_sink["Dim_Date_Key"])
df1.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Old Records

# COMMAND ----------

df_old= df1.filter(df1["Dim_Date_Key"].isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## New Records

# COMMAND ----------

df_new= df1.filter(df1["Dim_Date_Key"].isNull())
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Maximum Dim_Date_Key

# COMMAND ----------

if incremental_flag == '0':
    max_value=1
else:
    max_value = df_old.agg(F.max("Dim_Date_Key")).collect()[0][0]

max_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Surrogate Key

# COMMAND ----------

df = df_new.withColumn("Dim_Date_Key", max_value + F.monotonically_increasing_id())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Of Old and New(df with surrogate key values)

# COMMAND ----------

df= df_old.union(df)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 1 using Merge(Upsert) with Delta Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

table_name= "gold.dim_date"
path= "abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_date"

if DeltaTable.isDeltaTable(spark, path):
    deltaTable= DeltaTable.forPath(spark, path)
    deltaTable.alias("t").merge(df.alias("s"), "t.Dim_Date_Key=s.Dim_Date_Key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
    print("The Table is Created")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_date`

# COMMAND ----------

