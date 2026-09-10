# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Ingest Customer Data
# MAGIC Ingests raw customer records from Azure Data Lake Storage Gen2 into the Bronze Delta layer.

# COMMAND ----------

# MAGIC %run "../Common/utils"

# COMMAND ----------

# MAGIC %run "../Common/logging"

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date (yyyy-MM-dd)")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import current_timestamp, input_file_name

# Simulated Azure AD service principal auth, secrets backed by Azure Key Vault
tenant_id = dbutils.secrets.get(scope="loanplatform-kv", key="azure-tenant-id")
client_id = dbutils.secrets.get(scope="loanplatform-kv", key="azure-client-id")
client_secret = dbutils.secrets.get(scope="loanplatform-kv", key="azure-client-secret")

credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
blob_service_client = BlobServiceClient(
    account_url="https://loanplatformdl.blob.core.windows.net",
    credential=credential,
)

# COMMAND ----------

RAW_MOUNT_POINT = "/mnt/raw/customers"
RAW_SOURCE = "abfss://raw@loanplatformdl.dfs.core.windows.net/customers"

if not any(m.mountPoint == RAW_MOUNT_POINT for m in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=RAW_SOURCE,
        mount_point=RAW_MOUNT_POINT,
        extra_configs={
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        },
    )

# COMMAND ----------

customer_raw_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{RAW_MOUNT_POINT}/{run_date}/customers.csv")
)

log_event("ingest_customer", f"Read {customer_raw_df.count()} raw customer rows for {run_date}")

# COMMAND ----------

BRONZE_PATH = "abfss://bronze@loanplatformdl.dfs.core.windows.net/customers"

(
    customer_raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.customers
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

log_event("ingest_customer", "Bronze customer write complete")