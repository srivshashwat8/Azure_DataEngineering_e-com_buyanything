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
# MAGIC ## Working on customer dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC     select *, row_number() over(partition by CustomerID order by OrderDate) as rnk
# MAGIC     from parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`
# MAGIC )
# MAGIC select CustomerID as CustomerId, CustomerName, Country
# MAGIC from cte
# MAGIC where rnk = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Dataframe- df_src having Dimensions 

# COMMAND ----------

df_src = spark.sql("""
                  with cte as (
    select *, row_number() over(partition by CustomerID order by OrderDate) as rnk
    from parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`
)
select CustomerID as CustomerId, CustomerName, Country
from cte
where rnk = 1 
""")

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Gold layer data - df_sink

# COMMAND ----------

from delta.tables import DeltaTable
path = "abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_Customer"

if DeltaTable.isDeltaTable(spark, path):
         df_sink =spark.sql(
             """
            select Dim_Customer_Key, CustomerID,CustomerName, Country
            from delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_Customer`
        
             """
            )
         print("Delta table exists")
else:
        df_sink= spark.createDataFrame([], schema= "Dim_Customer_Key int, CustomerID int, CustomerName string, Country string")
df_sink.display()


    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old and New records - Left Join

# COMMAND ----------

# DBTITLE 1,Old and New records - Left Join
df1= df_src.join(df_sink, df_src["CustomerId"] == df_sink["CustomerID"], "left").select(df_src["CustomerId"], df_src["CustomerName"], df_src["Country"], df_sink["Dim_Customer_Key"])
df1.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Old Records

# COMMAND ----------

df_old= df1.filter(df1["Dim_Customer_Key"].isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## New Records

# COMMAND ----------

df_new= df1.filter(df1["Dim_Customer_Key"].isNull())
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Maximum Dim_Customer_Key

# COMMAND ----------

if incremental_flag == '0':
    max_value=1
else:
    max_value = df_old.agg(F.max("Dim_Customer_Key")).collect()[0][0]

max_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Surrogate Key

# COMMAND ----------

df = df_new.withColumn("Dim_Customer_Key", max_value + F.monotonically_increasing_id())
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

table_name= "gold.dim_Customer"
path= "abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_Customer"

if DeltaTable.isDeltaTable(spark, path):
    deltaTable= DeltaTable.forPath(spark, path)
    deltaTable.alias("t").merge(df.alias("s"), "t.Dim_Customer_Key=s.Dim_Customer_Key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
    print("The Table is Created")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_Customer`

# COMMAND ----------

