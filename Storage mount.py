# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  #source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  source = "abfss://bronze@anoopstore12551.dfs.core.windows.net/",
  #mount_point = "/mnt/<mount-name>",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  #source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  source = "abfss://silver@anoopstore12551.dfs.core.windows.net/",
  #mount_point = "/mnt/<mount-name>",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  #source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  source = "abfss://gold@anoopstore12551.dfs.core.windows.net/",
  #mount_point = "/mnt/<mount-name>",
  mount_point = "/mnt/gold",
  extra_configs = configs)
