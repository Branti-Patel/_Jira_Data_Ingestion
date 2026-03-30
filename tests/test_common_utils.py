import pytest
from src.python.common_utils import build_url, api_call, fetch_paginated_data,apply_parser,create_df, write_table, log_pipeline_run
from unittest.mock import patch
from pyspark.sql import SparkSession
from unittest.mock import MagicMock
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()

# Test url Func
def test_build_url_basic():
    result = build_url("https://jira.com","/rest/api/3","/search/jql")
    assert result == "https://jira.com/rest/api/3/search/jql"

# None raises error
def test_build_url_none():
    with pytest.raises(ValueError):
        build_url(None,"/rest/api/3","/search/jql")

# Empty raises error
def test_build_url_empty_raises():
    with pytest.raises(ValueError):
        build_url("", "/rest/api/3", "/search/jql")


def test_build_url_not_empty():
    result = build_url("https://jira.com","/rest/api/3","/search/jql")
    assert result != ""

## API CALL
# Returns correct JSON

@patch('src.python.common_utils.requests.get')
def test_api_call_success(mock_get):

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"result": "ok"}

    result = api_call("fake_url", None, None)

    assert result == {"result": "ok"}


#  HTTP status raises error

@patch('src.python.common_utils.requests.get')
def test_api_call_fail(mock_get):

    mock_get.return_value.raise_for_status.side_effect = Exception("Api Failed")

    with pytest.raises(Exception):
        api_call("fake_url",None,None)

# To check if Correct Arguments Are Passed

@patch('src.python.common_utils.requests.get')
def test_api_call_params(mock_get):

    auth = "auth"
    headers = {"h": "v"}
    params = {"p": 1}

    api_call("url", auth=auth, headers=headers, params=params)

    mock_get.assert_called_once_with(   # built in method that It checks:the mock was called exactly one time and with the excat arg you passed  

        "url",
        auth=auth,
        headers=headers,
        params=params
    )

# 4. params=None Case

@patch('src.python.common_utils.requests.get')
def test_api_call_no_params(mock_get):
    api_call("url", None, None)

    mock_get.assert_called_once_with(
        "url",
        auth=None,
        headers=None,
        params=None
    )


## Pagination 

# Single Page Test

@patch("src.python.common_utils.api_call")
def test_fetch_single_page(mock_api):

    # API returns less than batch_size → stop
    mock_api.return_value = {"items": [1, 2, 3]}

    result = fetch_paginated_data(
        url="url",
        auth=None,
        headers=None,
        params={},
        data_key="items",
        batch_size=5
    )

    assert result == [1, 2, 3]
    assert mock_api.call_count == 1

#Multiple Pages Test
@patch("src.python.common_utils.api_call")
def test_fetch_multiple_pages(mock_api):

    mock_api.side_effect = [ 
        {"items": [1, 2, 3]},
        {"items": [4]}
    ]

    result = fetch_paginated_data(
        url="url",
        auth=None,
        headers=None,
        params={},
        data_key="items",
        batch_size=3
    )

    assert result == [1, 2, 3, 4]  # Combined results across pages

    assert mock_api.call_count == 2   # Ensure API was called exactly twice (for 2 pages)

from unittest.mock import patch
from src.python.common_utils import fetch_paginated_data

# Empty response

@patch("src.python.common_utils.api_call")
def test_fetch_empty_data(mock_api):

    mock_api.return_value = {"items": []}

    result = fetch_paginated_data(
        url="url",
        auth=None,
        headers=None,
        params={},
        data_key="items",
        batch_size=5
    )

    assert result == [] # Expect empty result because items list is empty

    assert mock_api.call_count == 1 # Should call api only once

# Different data_key Test 

@patch("src.python.common_utils.api_call")
def test_fetch_different_data_key(mock_api): 

    mock_api.return_value = {"values": [10, 20]} # a response where the API uses a different key, not "items"

    result = fetch_paginated_data(
        url="url",
        auth=None,
        headers=None,
        params={},
        data_key="values",   # this tells the function which key to extract
        batch_size=5
    )

    assert result == [10, 20]       # Correct extraction
    assert mock_api.call_count == 1 # Only one page fetched

# Verifies orignal params is not changed.
@patch("src.python.common_utils.api_call")
def test_original_params_not_modified(mock_api):

    mock_api.return_value = {"items": []}

    original_params = {"project": "ABC"}   # input dict
    params_before = original_params.copy() # copy for comparison

    fetch_paginated_data(
        url="url",
        auth=None,
        headers=None,
        params=original_params,
        data_key="items",
        batch_size=5
    )

    assert original_params == params_before
    assert mock_api.call_count == 1

def test_apply_parser_basic():

    data = [1, 2, 3]

    def parser(x):
        return x * 2

    result = apply_parser(data, parser)

    assert result == [2, 4, 6]
# Parser Func 
def test_apply_parser_basic():

    data = [1, 2, 3]

    def parser(x):
        return x * 2

    result = apply_parser(data, parser)

    assert result == [2, 4, 6]

# Empty list


def test_apply_parser_empty():

    def parser(x):
        return x * 2

    result = apply_parser([], parser)

    assert result == []

# Parser Raises Error
def test_apply_parser_error():

    def parser(x):
        raise ValueError("Bad data")

    with pytest.raises(ValueError):
        apply_parser([1, 2], parser)

## Data Frame 

# No Defined schema


def test_create_df_no_schema():

    data = [{"id": 1}]  # define test data explicitly

    spark = MagicMock()                     # mock Spark session
    spark.createDataFrame.return_value = "fake_df"   # Defines what create dataframe must return

    result = create_df(spark, data)

    assert result == "fake_df"

# With Schema

def test_create_df_with_schema():

    # test schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])

    # test data
    data = [{"id": 1, "name": "Alice"}]

    spark = MagicMock()
    spark.createDataFrame.return_value = "fake_df"

    result = create_df(spark, data, schema=schema)

    assert result == "fake_df"    # result must be mock return value
# Ensure createDataFrame was called exactly once with the correct data and schema
    spark.createDataFrame.assert_called_once_with(data, schema=schema)


## Empty data returns empty DF
def test_create_df_empty_data():

    data = []                         # empty input
    spark = MagicMock()               # fake Spark session
    spark.createDataFrame.return_value = "empty_df"  # mock return

    result = create_df(spark, data)

    assert result == "empty_df"       # function returns mocked empty DF
    spark.createDataFrame.assert_called_once_with(data)  

## Write table 

# write was triggered

def test_write_table_called():

    mock_df = MagicMock()
    
    write_table(mock_df, "jira.issues_bronze")
    
    mock_df.write.format.assert_called_once() 

# delta format used
def test_write_table_correct_format():
    mock_df = MagicMock()
    
    write_table(mock_df, "jira.issues_bronze")
    
    mock_df.write.format.assert_called_once_with("delta")

# correct table name used
def test_write_table_correct_table_name():
    mock_df = MagicMock()
    
    write_table(mock_df, "jira.issues_bronze")
    
    mock_df.write.format.return_value \
        .mode.return_value \
        .option.return_value \
        .saveAsTable.assert_called_once_with("jira.issues_bronze")
    
