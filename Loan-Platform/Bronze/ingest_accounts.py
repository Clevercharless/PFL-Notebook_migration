# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Ingest Accounts Data
# MAGIC Ingests raw account event JSON files landed in Azure Blob Storage into Bronze Delta.

# COMMAND ----------

# MAGIC %run "../Common/utils"

# COMMAND ----------

# MAGIC %run "../Common/logging"

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date (yyyy-MM-dd)")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import current_timestamp, input_file_name

# Azure Blob Storage container holding daily account event exports
storage_account_name = "loanplatformdl"
storage_account_key = dbutils.secrets.get(scope="loanplatform-kv", key="blob-storage-key")

blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=storage_account_key,
)

ACCOUNTS_BLOB_PATH = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/accounts/{run_date}"

# COMMAND ----------

accounts_raw_df = (
    spark.read.format("json")
    .option("multiLine", True)
    .load(ACCOUNTS_BLOB_PATH)
)

log_event("ingest_accounts", f"Read {accounts_raw_df.count()} raw account event rows for {run_date}")

# COMMAND ----------

BRONZE_PATH = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/accounts"

(
    accounts_raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.accounts
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

log_event("ingest_accounts", "Bronze accounts write complete")
