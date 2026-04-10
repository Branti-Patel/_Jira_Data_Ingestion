# Databricks notebook source
# DBTITLE 1,Base URL & API Versions
JIRA_BASE = "https://rajdeepmatieda.atlassian.net"
API_V3 = "/rest/api/3"
API_AGILE = "/rest/agile/1.0"
API_GREENHOPPER = "/rest/greenhopper/1.0"

# COMMAND ----------

# DBTITLE 1,Endpoint Paths
ENDPOINTS = {
    "issues":        "/search/jql",
    "projects":      "/project/search",
    "boards":        "/board",
    "sprints":       "/board/{board_id}/sprint",
    "sprint_report": "/rapid/charts/sprintreport",
}
# Version mapping — which API version each endpoint uses
ENDPOINT_VERSIONS = {
    "issues":        API_V3,
    "projects":      API_V3,
    "boards":        API_AGILE,
    "sprints":       API_AGILE,
    "sprint_report": API_GREENHOPPER,
}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  TODO: Replace with dbutils.secrets.get() for production
# MAGIC

# COMMAND ----------

# DBTITLE 1,Credentials

JIRA_USER = "rajdeep.matieda@gmail.com"
JIRA_API_TOKEN = ""
AUTH = (JIRA_USER, JIRA_API_TOKEN)
HEADERS = {"Accept": "application/json"}

# COMMAND ----------

# DBTITLE 1,Target Database
CATALOG = "jira_data"
SCHEMA = "2generalized"
TARGET_DATABASE = f"{CATALOG}.{SCHEMA}"
LOG_TABLE = "pipeline_run_log"
