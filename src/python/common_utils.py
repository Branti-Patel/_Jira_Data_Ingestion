import requests
from datetime import datetime
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F

def api_call(url,auth,headers,params=None):
    response = requests.get(url,auth=auth,headers=headers,params=params)
    response.raise_for_status()
    return response.json()

def build_url(base,version,endpoint):
    if not base or not version or not endpoint:
        raise ValueError("Invalid URL")

    return base+version+endpoint

def fetch_paginated_data(url,auth,headers,params,data_key,batch_size=100):
    all_data =[]
    start =0 

    while True:
        params_local = params.copy()  # addon on the original params
        params_local["startAt"]=start
        params_local["maxResults"]= batch_size

        data = api_call(url, auth, headers,params_local)      #call the api function
        items = data.get(data_key, [])      #data_key("issues",values) differs as per the api req
        all_data.extend(items)

        if len(items) < batch_size: # not "isLast" bcuase not all api's return flag islast.
             break
       

        start += batch_size

    return all_data 

def apply_parser(data, parser_func):
    return [parser_func(item) for item in data]


def create_df(spark, data, schema=None):
    if schema:
        return spark.createDataFrame(data, schema=schema)
    else:
        return spark.createDataFrame(data)

def write_table(df, table_name):
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
def log_pipeline_run(
    spark,
    table_name,
    pipeline_name,
    status,
    records_processed,
    projects_processed=0
):
    run_data = [{
        "run_timestamp": datetime.now(),
        "pipeline_name": pipeline_name,
        "status": status,
        "records_processed": records_processed,
        "projects_processed": projects_processed
    }]

    schema = StructType([
        StructField("run_timestamp", TimestampType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("records_processed", IntegerType(), True),
        StructField("projects_processed", IntegerType(), True)
    ])

    df = spark.createDataFrame(run_data, schema=schema)

    write_table(df, table_name)


