# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish connection

# COMMAND ----------

spark.conf.set(   "fs.azure.account.key.cfstorageadlsgen2.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_KEY>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Customer

# COMMAND ----------

df_customer = spark.sql("""
SELECT * FROM delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_Customer`

""")

df_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Product 

# COMMAND ----------

df_product = spark.sql("""
SELECT * FROM delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_product`

""")

df_product.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Date

# COMMAND ----------

df_date = spark.sql("""
SELECT * FROM delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_date`

""")

df_date.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim Region

# COMMAND ----------

df_region = spark.sql("""
SELECT * FROM delta.`abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_region`

""")

df_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

silver_df = spark.sql("""
                      
                      SELECT * FROM parquet.`abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata`
                      """)

silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Fact Table

# COMMAND ----------

fact_df = (
    silver_df.join(df_customer, silver_df['CustomerID'] == df_customer['CustomerId'], 'left')
                    .join(df_product, silver_df['ProductID'] == df_product['ProductID'], 'left')
                    .join(df_date, silver_df['OrderDate'] == df_date['OrderDate'], 'left')
                    .join(df_region, silver_df['SalesRegion'] == df_region['SalesRegion'], 'left')
                    .select(df_customer['Dim_Customer_Key'], df_product['Dim_Product_Key'], df_date['Dim_Date_Key'], df_region['Dim_Region_Key'], silver_df['UnitPrice'], silver_df['TotalPrice'], silver_df['Quantity'])
)
display(fact_df)


# COMMAND ----------

from delta.tables import DeltaTable

table_name= "gold.dim_factsales"
path= "abfss://gold@cfstorageadlsgen2.dfs.core.windows.net/dim_factsales"

if DeltaTable.isDeltaTable(spark, path):
    deltaTable= DeltaTable.forPath(spark, path)
    deltaTable.alias("t").merge(fact_df.alias("s"), "t.Dim_Customer_Key=s.Dim_Customer_Key and t.Dim_Product_Key=s.Dim_Product_Key and t.Dim_Date_Key=s.Dim_Date_Key and  t.Dim_Region_Key=s.Dim_Region_Key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    fact_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
    print("The Table is Created")