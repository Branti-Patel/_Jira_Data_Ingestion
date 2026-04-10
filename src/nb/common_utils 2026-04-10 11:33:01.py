# Databricks notebook source
# DBTITLE 1,Imports
import requests
import json
import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,API Functions Header
# MAGIC %md
# MAGIC ## 1. API Functions

# COMMAND ----------

# DBTITLE 1,Build URL
def build_url(base, version, endpoint):
  
    return f"{base}{version}{endpoint}"

# COMMAND ----------

# DBTITLE 1,API GET with Retry
def api_get_with_retry(url, auth, headers, params=None, max_retries=3):
  
    if params is None:
        params = {}
    retryable_codes = {429, 500, 502, 503, 504}
    for attempt in range(max_retries + 1):
        response = requests.get(url, auth=auth, headers=headers, params=params)
        # Success — return immediately
        if response.ok:
            return response.json()
        # Retryable error — wait and retry
        if response.status_code in retryable_codes and attempt < max_retries:
            wait_time = 2 ** attempt  # 1, 2, 4, 8 seconds
            time.sleep(wait_time)
            continue
        # Non-retryable error OR max retries exhausted — raise
        response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Fetch Paginated Data
def fetch_paginated_data(url, auth, headers, params, data_key, batch_size=100):
   
    all_data = []
    start = 0
    while True:
        # Copy params so we don't mutate the original
        params_local = params.copy()
        params_local["startAt"] = start
        params_local["maxResults"] = batch_size
       
        # Uses retry-enabled API call
        data = api_get_with_retry(url, auth, headers, params_local)
        items = data.get(data_key, [])
        all_data.extend(items) #  returns the value stored under that key
       
        # If fewer items than batch_size → last page
        if len(items) < batch_size:
            break
        start += batch_size  # Move to next page
    return all_data

# COMMAND ----------

# DBTITLE 1,Data Functions Header
# MAGIC %md
# MAGIC ## 2. Data Functions

# COMMAND ----------

# DBTITLE 1,Create Raw Rows
def create_raw_rows(records):
   
    return [{"raw_json": json.dumps(record)} for record in records]

# COMMAND ----------

# DBTITLE 1,Parse JSON Column
def parse_json_column(df, schema, column_mappings, json_col="raw_json"):
   
    # Step 1: Parse JSON string into struct
    df_parsed = df.withColumn("parsed", F.from_json(F.col(json_col), schema))
    # Step 2: Select and alias each column
    select_exprs = [
        F.col(source_path).alias(target_alias)
        for source_path, target_alias in column_mappings.items()
    ]
    return df_parsed.select(*select_exprs)

# COMMAND ----------

# DBTITLE 1,Handle Nulls
def handle_nulls(df, null_rules):
   
    for col_name, default_value in null_rules.items():
        if isinstance(default_value, str):
            df = df.withColumn(
                col_name,
                F.when(
                    (F.col(col_name).isNull()) | (F.trim(F.col(col_name)) == ""),
                    F.lit(default_value)
                ).otherwise(F.trim(F.col(col_name)))
            )
        else:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(default_value))
                .otherwise(F.col(col_name))
            )
    return df

# COMMAND ----------

# DBTITLE 1,Dedup DataFrame
def dedup_dataframe(df, key_columns):
   
    before = df.count()
    df_dedup = df.dropDuplicates(key_columns)
    after = df_dedup.count()
    return df_dedup

# COMMAND ----------

# DBTITLE 1,Validate DataFrame
def validate_df(df, key_column):
   
    row_count = df.count()
    if row_count == 0:
        raise ValueError("Validation failed: DataFrame is empty")
    null_count = df.filter(F.col(key_column).isNull()).count()
    if null_count > 0:
        raise ValueError(f"Validation failed: {null_count} nulls found in key column '{key_column}'")
  

# COMMAND ----------

# DBTITLE 1,Write Functions Header
# MAGIC %md
# MAGIC ## 3. Write Functions

# COMMAND ----------

# DBTITLE 1,Write Table
def write_table(spark, df, table_name, mode="append", merge_key=None):
   
    if mode == "append":
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
        
    elif mode == "merge":
        from delta.tables import DeltaTable
        try:
            # Table exists → merge (upsert)
            delta_table = DeltaTable.forName(spark, table_name)
            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{merge_key} = source.{merge_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
           
        except Exception:
            # Table doesn't exist yet → create with overwrite
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
            )
           
    elif mode == "overwrite":
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        

# COMMAND ----------

# DBTITLE 1,Pipeline Logging Header
# MAGIC %md
# MAGIC ## 4. Pipeline Logging

# COMMAND ----------

# DBTITLE 1,Log Pipeline Run
def log_pipeline_run(spark, table_name, pipeline_name, status, records_processed, mode="append"):
   
    run_data = [{
        "run_timestamp":      datetime.now(),
        "pipeline_name":      pipeline_name,
        "status":             status,
        "records_processed":  records_processed,
    }]
    schema = StructType([
        StructField("run_timestamp",     TimestampType(), True),
        StructField("pipeline_name",     StringType(),    True),
        StructField("status",            StringType(),    True),
        StructField("records_processed", IntegerType(),   True),
    ])
    df = spark.createDataFrame(run_data, schema=schema)
    write_table(spark, df, table_name, mode=mode)

# COMMAND ----------

# DBTITLE 1,Get Last Run Timestamp
def get_last_run_timestamp(spark, database, log_table, pipeline_name):
  
    try:
        df_log = spark.table(f"{database}.{log_table}")
        last_run = (
            df_log
            .filter(F.col("status") == "SUCCESS")
            .filter(F.col("pipeline_name") == pipeline_name)
            .select(F.max("run_timestamp").alias("last_run"))
            .collect()[0]["last_run"]
        )
        if last_run:
            last_run_str = last_run.strftime("%Y-%m-%d %H:%M")
        
            return last_run_str
        else:
           
            return None
    except Exception:
       
        return None

# COMMAND ----------

# DBTITLE 1,Helper Functions Header
# MAGIC %md
# MAGIC ## 5. Helper — Build Params from Config

# COMMAND ----------

# DBTITLE 1,Build Params
def build_params(config, parent_value=None, last_run_time=None):
   
    params = {}
    # Add JQL from template if this entity depends on a parent
    if config.get("jql_template") and parent_value:
        jql = config["jql_template"].replace("{value}", str(parent_value))
        # Add incremental filter to JQL if enabled
        if config.get("incremental") and last_run_time:
            inc_field = config.get("incremental_field", "updated")
            jql += f' AND {inc_field} >= "{last_run_time}"'
        params["jql"] = jql
    # Add fields parameter if specified
    if config.get("fields"):
        params["fields"] = config["fields"]
    return params
