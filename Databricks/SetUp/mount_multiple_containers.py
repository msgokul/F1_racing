# Databricks notebook source
# MAGIC %md
# MAGIC ###Mounting all the containers in the storage account to the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Get the secret keys of client id/client scret value/ tenant id

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='service-principal-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='service-principle-client-secret-value')

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Write a function to mount the containers

# COMMAND ----------

def mount_adls(storage_acc_name, container_name):
  try:
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


    # if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
    if  f"/mnt/{storage_acc_name}/{container_name}" in [mount.mountPoint for mount in dbutils.fs.mounts()]:
      print("Mount already exists...")
      return
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_acc_name}/{container_name}",
      extra_configs = configs)
    print("Mounted successfully...")
  except Exception as e:
    print("Mount Failed - " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC Mount demo container

# COMMAND ----------

mount_adls("formula1gokul", "demo")

# COMMAND ----------

# MAGIC %md
# MAGIC Mount raw container

# COMMAND ----------

mount_adls("formula1gokul", "raw")

# COMMAND ----------

mount_adls("formula1gokul", "processed")
mount_adls("formula1gokul", "presentation")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

type(dbutils.fs.mounts())

# COMMAND ----------

