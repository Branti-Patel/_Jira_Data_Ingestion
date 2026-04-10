# Databricks notebook source
# DBTITLE 1,Import Shared Utilities
# MAGIC %md
# MAGIC ## Import Shared Utilities

# COMMAND ----------

# DBTITLE 1,Import API Config
# MAGIC %run ./api_config

# COMMAND ----------

# DBTITLE 1,Import Common Utils
# MAGIC %run ./common_utils

# COMMAND ----------

# DBTITLE 1,Import Entity Configs
# MAGIC %run ./entity_config

# COMMAND ----------

# DBTITLE 1,Additional Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Setup Header
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema
# Create catalog + schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}")

# This dict stores extracted values for child dependencies
# e.g. results["projects"] = ["PROJ1", "PROJ2", ...]
results = {}

# COMMAND ----------

# DBTITLE 1,Run All Pipelines Header
# MAGIC %md
# MAGIC ## Run All Pipelines

# COMMAND ----------

# DBTITLE 1,Main Pipeline Loop
for config in ALL_CONFIGS:
    entity_name = config["name"]
    
    try:
        # ── Step 1: Build URL ──
        version = ENDPOINT_VERSIONS[config["endpoint_key"]]
        url = build_url(JIRA_BASE, version, ENDPOINTS[config["endpoint_key"]])
        
        # ── Step 2: Get incremental timestamp ──
        last_run = None
        if config.get("incremental"):
            last_run = get_last_run_timestamp(spark, TARGET_DATABASE, LOG_TABLE, entity_name)
        
        # ── Step 3: Fetch data ──
        all_data = []
        if config["depends_on"]:
            # This entity depends on a parent (e.g. issues depends on projects)
            parent_values = results[config["depends_on"]]
            
            for value in parent_values:
                params = build_params(config, parent_value=value, last_run_time=last_run)
                batch = fetch_paginated_data(url, AUTH, HEADERS, params, config["data_key"])
                all_data.extend(batch)
        else:
            # No dependency — fetch all directly
            all_data = fetch_paginated_data(url, AUTH, HEADERS, {}, config["data_key"])
        
        # ── Step 4: Store parent values for children ──
        if config.get("parent_field"):
            field = config["parent_field"]
            results[entity_name] = [item[field] for item in all_data if item.get(field)]
        
        # ── Step 5: Write bronze table (if configured) ──
        if config["raw_table"] and len(all_data) > 0:
            raw_rows = create_raw_rows(all_data)
            df_raw = spark.createDataFrame(raw_rows, "raw_json STRING")
            df_raw = df_raw.withColumn("ingestion_timestamp", F.current_timestamp())
            
            write_table(
                spark, df_raw,
                f"{TARGET_DATABASE}.{config['raw_table']}",
                mode=config["raw_write_mode"]
            )
        
        # ── Step 6: Log success ──
        log_pipeline_run(
            spark,
            f"{TARGET_DATABASE}.{LOG_TABLE}",
            pipeline_name=entity_name,
            status="SUCCESS",
            records_processed=len(all_data)
        )
        
    except Exception as e:
        # Log failure but DON'T stop — continue to next entity
        log_pipeline_run(
            spark,
            f"{TARGET_DATABASE}.{LOG_TABLE}",
            pipeline_name=entity_name,
            status=f"FAILED: {str(e)[:200]}",
            records_processed=0
        )

# COMMAND ----------

# DBTITLE 1,Summary Header
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Display Summary
# Display pipeline completion summary
display(spark.sql(f"""
    SELECT 
        pipeline_name,
        status,
        records_processed,
        run_timestamp
    FROM {TARGET_DATABASE}.{LOG_TABLE}
    WHERE run_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
    ORDER BY run_timestamp DESC
"""))