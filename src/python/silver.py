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

# DBTITLE 1,Run Silver Pipeline Header
# MAGIC %md
# MAGIC ## Run Silver for All Entities

# COMMAND ----------

# DBTITLE 1,Main Silver Pipeline Loop
# Only process entities that have a silver_table defined
silver_configs = [c for c in ALL_CONFIGS if c.get("silver_table")]

for config in silver_configs:
    entity_name = config["name"]
    
    try:
        # ── Step 1: Read bronze table ──
        bronze_table = f"{TARGET_DATABASE}.{config['raw_table']}"
        df_bronze = spark.table(bronze_table)
        
        # ── Step 2: Parse JSON → flat columns (config-driven) ──
        df_silver = parse_json_column(
            df_bronze,
            config["schema"],
            config["column_mappings"]
        )
        
        # ── Step 3: Handle nulls (config-driven) ──
        if config.get("null_rules"):
            df_silver = handle_nulls(df_silver, config["null_rules"])
        
        # ── Step 4: Dedup (config-driven) ──
        if config.get("dedup_keys"):
            df_silver = dedup_dataframe(df_silver, config["dedup_keys"])
        
        # ── Step 5: Validate before writing ──
        validate_df(df_silver, config["dedup_keys"][0])
        
        # ── Step 6: Write silver table ──
        silver_table = f"{TARGET_DATABASE}.{config['silver_table']}"
        write_table(
            spark, df_silver,
            silver_table,
            mode=config["silver_write_mode"],
            merge_key=config.get("merge_key")
        )
        
        # ── Step 7: Log success ──
        log_pipeline_run(
            spark,
            f"{TARGET_DATABASE}.{LOG_TABLE}",
            pipeline_name=f"{entity_name}_silver",
            status="SUCCESS",
            records_processed=df_silver.count()
        )
        
    except Exception as e:
        # Log failure but continue to next entity
        log_pipeline_run(
            spark,
            f"{TARGET_DATABASE}.{LOG_TABLE}",
            pipeline_name=f"{entity_name}_silver",
            status=f"FAILED: {str(e)[:200]}",
            records_processed=0
        )

# COMMAND ----------

# DBTITLE 1,Summary Header
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Display Summary
# Display silver pipeline completion summary
display(spark.sql(f"""
    SELECT 
        pipeline_name,
        status,
        records_processed,
        run_timestamp
    FROM {TARGET_DATABASE}.{LOG_TABLE}
    WHERE pipeline_name LIKE '%_silver'
        AND run_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
    ORDER BY run_timestamp DESC
"""))