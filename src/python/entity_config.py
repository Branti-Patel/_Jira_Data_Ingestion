# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Projects Config Header
# MAGIC %md
# MAGIC ## 1. Projects Config

# COMMAND ----------

# DBTITLE 1,Projects Config
PROJECTS_CONFIG = {
    # Identity
    "name":              "projects",
    # API
    "endpoint_key":      "projects",
    "api_version":       "version",       # uses API_V3
    "data_key":          "values",
    "fields":            None,
    # Dependency
    "depends_on":        None,
    "parent_field":      "key",           # extract "key" from results for children
    "jql_template":      None,
    # Bronze
    "raw_table":         None,            # no raw table for projects (just keys)
    "raw_write_mode":    None,
    # Silver (no silver for projects — they are just a dependency)
    "schema":            None,
    "column_mappings":   None,
    "null_rules":        None,
    "dedup_keys":        None,
    "silver_table":      None,
    "silver_write_mode": None,
    "merge_key":         None,
    # Incremental
    "incremental":       False,
    "incremental_field": None,
}

# COMMAND ----------

# DBTITLE 1,Issues Config Header
# MAGIC %md
# MAGIC ## 2. Issues Config

# COMMAND ----------

# DBTITLE 1,Issues Schema
# Schema for raw issue JSON from /rest/api/3/search/jql
ISSUES_SCHEMA = StructType([
    StructField("expand", StringType()),
    StructField("id", StringType()),
    StructField("key", StringType()),
    StructField("fields", StructType([
        StructField("summary", StringType()),
        StructField("description", StructType([
            StructField("type", StringType()),
            StructField("version", StringType())
        ])),
        StructField("issuetype", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("iconUrl", StringType()),
            StructField("self", StringType())
        ])),
        StructField("status", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("description", StringType()),
            StructField("iconUrl", StringType()),
            StructField("self", StringType()),
            StructField("statusCategory", StructType([
                StructField("id", StringType()),
                StructField("key", StringType()),
                StructField("name", StringType()),
                StructField("colorName", StringType()),
                StructField("self", StringType())
            ]))
        ])),
        StructField("assignee", StructType([
            StructField("accountId", StringType()),
            StructField("displayName", StringType()),
            StructField("emailAddress", StringType()),
            StructField("accountType", StringType()),
            StructField("timeZone", StringType()),
            StructField("self", StringType())
        ])),
        StructField("reporter", StructType([
            StructField("accountId", StringType()),
            StructField("displayName", StringType()),
            StructField("emailAddress", StringType()),
            StructField("accountType", StringType()),
            StructField("timeZone", StringType()),
            StructField("self", StringType())
        ])),
        StructField("priority", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("iconUrl", StringType()),
            StructField("self", StringType())
        ])),
        StructField("parent", StructType([
            StructField("id", StringType()),
            StructField("key", StringType()),
            StructField("self", StringType())
        ])),
        StructField("customfield_10020", ArrayType(
            StructType([
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("state", StringType()),
                StructField("startDate", StringType()),
                StructField("endDate", StringType())
            ])
        )),
        StructField("customfield_10016", DoubleType())
    ]))
])

# COMMAND ----------

# DBTITLE 1,Issues Column Mappings and Null Rules
# Column mappings: parsed JSON path → Silver column name
ISSUES_COLUMN_MAPPINGS = {
    "parsed.id":                                    "issue_id",
    "parsed.key":                                   "issue_key",
    "parsed.fields.summary":                        "summary",
    "parsed.fields.issuetype.name":                 "issue_type",
    "parsed.fields.status.name":                    "status_name",
    "parsed.fields.status.statusCategory.name":     "status_category",
    "parsed.fields.assignee.displayName":           "assignee_name",
    "parsed.fields.assignee.emailAddress":          "assignee_email",
    "parsed.fields.reporter.displayName":           "reporter_name",
    "parsed.fields.priority.name":                  "priority",
    "parsed.fields.parent.key":                     "parent_key",
    "parsed.fields.customfield_10016":              "story_points",
    "parsed.fields.customfield_10020":              "sprints",
}

# Null handling rules for Silver
ISSUES_NULL_RULES = {
    "assignee_name":  "Unassigned",
    "assignee_email": "N/A",
    "reporter_name":  "Unknown",
    "story_points":   0.0,
    "parent_key":     "No Parent",
    "priority":       "None",
}

# COMMAND ----------

# DBTITLE 1,Issues Config
# The config dict
ISSUES_CONFIG = {
    # Identity
    "name":              "issues",
    # API
    "endpoint_key":      "issues",
    "api_version":       "version",
    "data_key":          "issues",
    "fields":            "summary,description,issuetype,status,assignee,reporter,priority,parent,customfield_10020,customfield_10016",
    # Dependency — issues depend on projects
    "depends_on":        "projects",
    "parent_field":      None,
    "jql_template":      'project = "{value}"',
    # Bronze
    "raw_table":         "issues_raw",
    "raw_write_mode":    "append",
    # Silver
    "schema":            ISSUES_SCHEMA,
    "column_mappings":   ISSUES_COLUMN_MAPPINGS,
    "null_rules":        ISSUES_NULL_RULES,
    "dedup_keys":        ["issue_id"],
    "silver_table":      "issues_silver",
    "silver_write_mode": "merge",
    "merge_key":         "issue_id",
    # Incremental
    "incremental":       True,
    "incremental_field": "updated",
}

# COMMAND ----------

# DBTITLE 1,All Configs Header
# MAGIC %md
# MAGIC ## 3. All Configs List (controls execution order)

# COMMAND ----------

# DBTITLE 1,All Configs List
# Order matters! Parents must come before children.
ALL_CONFIGS = [
    PROJECTS_CONFIG,       # 1st — no dependency
    ISSUES_CONFIG,         # 2nd — depends on projects
    # BOARDS_CONFIG,       # TODO: add later
    # SPRINTS_CONFIG,      # TODO: add later (depends on boards)
]