# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Establish Connection

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.cfstorageadlsgen2.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_KEY>"
)

# COMMAND ----------

filename=dbutils.fs.ls("abfss://bronze@cfstorageadlsgen2.dfs.core.windows.net/")[0].name
filename

# COMMAND ----------

df = spark.read.parquet(f"abfss://bronze@cfstorageadlsgen2.dfs.core.windows.net/{filename}")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to titlecase - initCap

# COMMAND ----------

df = df.withColumn("Country", F.initcap(df.Country))
df = df.withColumn("ProductCategory", F.initcap(df.ProductCategory))
df = df.withColumn("ProductName", F.initcap(df.ProductName))
df = df.withColumn("SalesRegion", F.initcap(df.SalesRegion))
df = df.withColumn("CustomerName", F.initcap(df.CustomerName))


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove rows where all values are missing

# COMMAND ----------

df = df.dropna(how='all')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove duplicates

# COMMAND ----------

df= df.dropDuplicates()
df.display()

# COMMAND ----------

df.write.format("parquet").mode("overwrite").option("path", "abfss://silver@cfstorageadlsgen2.dfs.core.windows.net/silverdata").save()

# COMMAND ----------



# COMMAND ----------

