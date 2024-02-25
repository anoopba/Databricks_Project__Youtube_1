# Databricks notebook source
dbutils.fs.ls("/mnt/bronze/SalesLT/Address/")

# COMMAND ----------

input_path = "/mnt/bronze/SalesLT/Address/Address.parquet"

# COMMAND ----------

df = spark.read.format('parquet').load(input_path)

# COMMAND ----------

from pyspark.sql.functions import col,date_format
from pyspark.sql.types import TimestampType

df = df.withColumn('ModifiedDate',date_format(col('ModifiedDate'),'yyyy-MM-dd'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Doing Transformation for all the df's

# COMMAND ----------

#Lets understand how to access the dbutils files data.
file_path = dbutils.fs.ls("mnt/bronze/SalesLT/")

#Lets check the Type of the file_path
print(type(file_path))
print(file_path)
print("\n")
#Lets check the first index path type and the type of it
first_index_file_path = dbutils.fs.ls("mnt/bronze/SalesLT/")[0]
print(type(first_index_file_path))
print(first_index_file_path)
print("\n")
#Lets access the <class 'dbruntime.dbutils.FileInfo'> data
print("name : " + first_index_file_path.name)
print("path :" + first_index_file_path.path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Below we are trying to fetch the list of table names

# COMMAND ----------

table_names = []

for i in dbutils.fs.ls("mnt/bronze/SalesLT/"):
    table_names.append(i.name.split('/')[0])

print(table_names)

# COMMAND ----------


for table_name in table_names:
    path_to_read = "/mnt/bronze/SalesLT/" + table_name + "/" + table_name + ".parquet"
    df = spark.read.format('parquet').load(path_to_read)
    columns = df.columns
    for col in columns:
        if "date" in col or "Date" in col:
            df = df.withColumn('ModifiedDate',date_format(df[col],'yyyy-MM-dd'))
        
    output_path = "/mnt/silver/SalesLT/" + table_name + "/"
    df.write.format('delta').mode('overwrite').save(output_path)
