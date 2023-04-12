# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Tier
# MAGIC 
# MAGIC In the last exercise, we took our data wrote it to the Parquet format, ready for us to pick up in the Silver Tier. In this exercise, we'll take our first step towards curation and cleanup by:
# MAGIC * Unpacking strings containing json to JSON
# MAGIC * Flattening our data (unpack nested structures and bring to top level)
# MAGIC 
# MAGIC We'll do this for:
# MAGIC * StartTransaction Request
# MAGIC * StartTransaction Response
# MAGIC * StopTransaction Request
# MAGIC * MeterValues Request

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "batch_processing_silver"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the bronze layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_bronze_ingest")
print(input_dir)


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(f"{input_dir}/output/")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

def start_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = None
    message_type = None
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_request_filter))

# COMMAND ----------

########## SOLUTION ###########

def start_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = "StartTransaction"
    message_type = 2
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_start_transaction_request_filter_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123"
    },
    {
        "action": "StartTransaction",
        "message_type": 3,
        "charge_point_id": "123"
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_data = [ (x.action, x.message_type) for x in result.collect()]
    expected_data = [("StartTransaction", 2)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    print("All tests pass! :)")
    
test_start_transaction_request_filter_unit(spark, start_transaction_request_filter)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_start_transaction_request_filter_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    display(result)

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_action = [ x.action for x in result.select("action").distinct().collect()]
    expected_action = ["StartTransaction"]
    assert result_action == expected_action, f"Expected {expected_action}, but got {result_action}"
    
    result_message_type = [ x.message_type for x in result.select("message_type").distinct().collect()]
    expected_message_type = [2]
    assert result_message_type == expected_message_type, f"Expected {expected_message_type}, but got {result_message_type}"

    print("All tests pass! :)")
    
test_start_transaction_request_filter_e2e(df.transform(start_transaction_request_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- meter_start: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql.functions import from_json, col

def start_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json))

# COMMAND ----------

########## SOLUTION ###########

from pyspark.sql.functions import from_json, col

def start_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), body_schema))
    ###
    
display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_request_unpack_json_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "connector_id": 1,
            "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "meter_start": 0,
            "timestamp": "2022-01-01T08:00:00+00:00",
            "reservation_id": None
        })
    },
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "connector_id": 1,
            "id_tag": "7f72c19c-5b36-400e-980d-7a16d30ca490",
            "meter_start": 0,
            "timestamp": "2022-01-01T09:00:00+00:00",
            "reservation_id": None
        })
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField('action', StringType(), True), 
            StructField('message_type', IntegerType(), True), 
            StructField('charge_point_id', StringType(), True), 
            StructField('body', StringType(), True), 
            StructField('new_body', StructType([
                         StructField('connector_id', IntegerType(), True), 
                         StructField('id_tag', StringType(), True), 
                         StructField('meter_start', IntegerType(), True), 
                         StructField('timestamp', StringType(), True), 
                         StructField('reservation_id', IntegerType(), True)
            ]), True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.timestamp for x in result.collect()]
    expected_data = ['2022-01-01T08:00:00+00:00', '2022-01-01T09:00:00+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_request_unpack_json_unit(spark, start_transaction_request_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_request_unpack_json_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body', StructType([
            StructField('connector_id', IntegerType(), True), 
            StructField('id_tag', StringType(), True), 
            StructField('meter_start', IntegerType(), True), 
            StructField('timestamp', StringType(), True), 
            StructField('reservation_id', IntegerType(), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.timestamp for x in result.sort(col("new_body.timestamp")).limit(3).collect()]
    expected_data = ['2023-01-01T10:43:09.900215+00:00', '2023-01-01T11:20:31.296429+00:00', '2023-01-01T14:03:42.294160+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_request_unpack_json_e2e(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

def start_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten))

# COMMAND ----------

############ SOLUTION ##############

def start_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df.\
        withColumn("connector_id", input_df.new_body.connector_id).\
        withColumn("id_tag", input_df.new_body.connector_id).\
        withColumn("meter_start", input_df.new_body.meter_start).\
        withColumn("timestamp", input_df.new_body.timestamp).\
        withColumn("reservation_id", input_df.new_body.reservation_id).\
        drop("new_body").\
        drop("body")
    ###

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

def test_start_transaction_request_flatten_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "connector_id": 1,
            "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "meter_start": 0,
            "timestamp": "2022-01-01T08:00:00+00:00",
            "reservation_id": None
        })
    },
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "connector_id": 1,
            "id_tag": "7f72c19c-5b36-400e-980d-7a16d30ca490",
            "meter_start": 0,
            "timestamp": "2022-01-01T09:00:00+00:00",
            "reservation_id": None
        })
    },
    ])

    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    
    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    ).withColumn("new_body", from_json(col("body"), body_schema))
    
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('action', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('connector_id', IntegerType(), True), 
        StructField('id_tag', IntegerType(), True), 
        StructField('meter_start', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('reservation_id', IntegerType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.collect()]
    expected_data = ['2022-01-01T08:00:00+00:00', '2022-01-01T09:00:00+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_request_flatten_unit(spark, start_transaction_request_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_request_flatten_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True),
        StructField('connector_id', IntegerType(), True), 
        StructField('id_tag', IntegerType(), True), 
        StructField('meter_start', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('reservation_id', IntegerType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.sort(col("timestamp")).limit(3).collect()]
    expected_data = ['2023-01-01T10:43:09.900215+00:00', '2023-01-01T11:20:31.296429+00:00', '2023-01-01T14:03:42.294160+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_request_flatten_e2e(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Response

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Response" (`3`) message_type.

# COMMAND ----------

def start_transaction_response_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = None
    message_type = None
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_response_filter))

# COMMAND ----------

############## SOLUTION ################

def start_transaction_response_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = "StartTransaction"
    message_type = 3
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_response_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_start_transaction_response_filter_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123"
    },
    {
        "action": "StartTransaction",
        "message_type": 3,
        "charge_point_id": "123"
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_data = [ (x.action, x.message_type) for x in result.collect()]
    expected_data = [("StartTransaction", 3)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    print("All tests pass! :)")
    
test_start_transaction_response_filter_unit(spark, start_transaction_response_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_start_transaction_response_filter_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    display(result)

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_action = [ x.action for x in result.select("action").distinct().collect()]
    expected_action = ["StartTransaction"]
    assert result_action == expected_action, f"Expected {expected_action}, but got {result_action}"
    
    result_message_type = [ x.message_type for x in result.select("message_type").distinct().collect()]
    expected_message_type = [3]
    assert result_message_type == expected_message_type, f"Expected {expected_message_type}, but got {result_message_type}"

    print("All tests pass! :)")
    
test_start_transaction_response_filter_e2e(df.transform(start_transaction_response_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- id_tag_info: struct (nullable = true)
# MAGIC  |    |    |-- status: string (nullable = true)
# MAGIC  |    |    |-- parent_id_tag: string (nullable = true)
# MAGIC  |    |    |-- expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

def start_transaction_response_unpack_json(input_df: DataFrame):
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])
    ### YOUR CODE HERE
    return input_df
    ###
    
display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json))

# COMMAND ----------

########### SOLUTION ###########

def start_transaction_response_unpack_json(input_df: DataFrame):
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), body_schema))
    ###
    
display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_response_unpack_json_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "message_id": "456",
            "body": json.dumps({
                "transaction_id": 1,
                "id_tag_info": {
                    "status": "Accepted",
                    "parent_id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
                    "expiry_date": None
                }
            })
        },
    ])
    
    body_id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", body_id_tag_info_schema, True)
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("message_id", StringType()),
            StructField("body", StringType()),
        ])
    ).withColumn("new_body",from_json(col("body"), body_schema))

    
    result = input_df.transform(f)

    print("Transformed DF:")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True), 
        StructField('message_id', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body', StructType([
            StructField('transaction_id', IntegerType(), True), 
            StructField('id_tag_info', StructType([
                StructField('status', StringType(), True), 
                StructField('parent_id_tag', StringType(), True), 
                StructField('expiry_date', StringType(), True)
            ]), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.id_tag_info.parent_id_tag for x in result.collect()]
    expected_data = ['ea068c10-1bfb-4128-ab88-de565bd5f02f']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_response_unpack_json_unit(spark, start_transaction_response_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_response_unpack_json_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body', StructType([
            StructField('transaction_id', IntegerType(), True), 
            StructField('id_tag_info', StructType([
                StructField('status', StringType(), True), 
                StructField('parent_id_tag', StringType(), True), 
                StructField('expiry_date', StringType(), True)
            ]), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.id_tag_info.parent_id_tag for x in result.sort(col("message_id")).limit(3).collect()]
    expected_data = ['0c806549-afb1-4cb4-8b36-77f088e0f273', 'b7bc3b31-5b0d-41b5-b0bf-762ac9b785ed', '9495b2ac-d3ef-4330-a098-f1661ab9303e']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_response_unpack_json_e2e(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- id_tag_info_status: string (nullable = true)
# MAGIC  |-- id_tag_info_parent_id_tag: string (nullable = true)
# MAGIC  |-- id_tag_info_expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

def start_transaction_response_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten))

# COMMAND ----------

########### SOLUTION ###########

def start_transaction_response_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df.\
        withColumn("transaction_id", input_df.new_body.transaction_id).\
        withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status).\
        withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag).\
        withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date).\
        drop("new_body").\
        drop("body")
    ###

display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

def test_start_transaction_response_flatten_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "action": "StartTransaction",
            "message_type": 3,
            "charge_point_id": "123",
            "body": json.dumps({
                "transaction_id": 1,
                "id_tag_info": {
                    "status": "Accepted",
                    "parent_id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
                    "expiry_date": None
                }
            })
        },
        {
            "action": "StartTransaction",
            "message_type": 3,
            "charge_point_id": "123",
            "body": json.dumps({
                "transaction_id": 2,
                "id_tag_info": {
                    "status": "Accepted",
                    "parent_id_tag": "74924177-d936-4898-8943-1c1a512d7f4c",
                    "expiry_date": None
                }
            })
        },
    ])

    body_id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", body_id_tag_info_schema, True)
    ])
    
    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    ).withColumn("new_body", from_json(col("body"), body_schema))
    
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('action', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('id_tag_info_status', StringType(), True), 
        StructField('id_tag_info_parent_id_tag', StringType(), True), 
        StructField('id_tag_info_expiry_date', StringType(), True)
    ])

    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.transaction_id for x in result.collect()]
    expected_data = [1, 2]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_response_flatten_unit(spark, start_transaction_response_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_start_transaction_response_flatten_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('id_tag_info_status', StringType(), True), 
        StructField('id_tag_info_parent_id_tag', StringType(), True), 
        StructField('id_tag_info_expiry_date', StringType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.transaction_id for x in result.sort(col("message_id")).limit(3).collect()]
    expected_data = [1652, 318, 1677]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_start_transaction_response_flatten_e2e(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StopTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Filter
# MAGIC In this exercise, filter for the `StopTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

def stop_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = None
    message_type = None
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(stop_transaction_request_filter))

# COMMAND ----------

############ SOLUTION #############

def stop_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = "StopTransaction"
    message_type = 2
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(stop_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_stop_transaction_request_filter_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StopTransaction",
        "message_type": 2,
        "charge_point_id": "123"
    },
    {
        "action": "StopTransaction",
        "message_type": 3,
        "charge_point_id": "123"
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_data = [ (x.action, x.message_type) for x in result.collect()]
    expected_data = [("StopTransaction", 2)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    print("All tests pass! :)")
    
test_stop_transaction_request_filter_unit(spark, stop_transaction_request_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_stop_transaction_request_filter_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    display(result)

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_action = [ x.action for x in result.select("action").distinct().collect()]
    expected_action = ["StopTransaction"]
    assert result_action == expected_action, f"Expected {expected_action}, but got {result_action}"
    
    result_message_type = [ x.message_type for x in result.select("message_type").distinct().collect()]
    expected_message_type = [2]
    assert result_message_type == expected_message_type, f"Expected {expected_message_type}, but got {result_message_type}"

    print("All tests pass! :)")
    
test_stop_transaction_request_filter_e2e(df.transform(stop_transaction_request_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- meter_stop: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- reason: string (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- transaction_data: array (nullable = true)
# MAGIC  |    |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import ArrayType
    
def stop_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
    ])
    ### YOUR CODE HERE
    return input_df
    ###


display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json))

# COMMAND ----------

############### SOLUTION ################

from pyspark.sql.types import ArrayType
    
def stop_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
    ])
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), body_schema))
    ###


display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_stop_transaction_request_unpack_json_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "StopTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "meter_stop": 2780,
            "timestamp": "2022-01-01T08:20:00+00:00",
            "transaction_id": 1,
            "reason": None,
            "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "transaction_data": None
        }),
    },
    {
        "action": "StartTransaction",
        "message_type": 2,
        "charge_point_id": "123",
        "body": json.dumps({
            "meter_stop": 5000,
            "timestamp": "2022-01-01T09:20:00+00:00",
            "transaction_id": 1,
            "reason": None,
            "id_tag": "25b72fa9-85fd-4a75-acbe-5a15fc7430a8",
            "transaction_data": None
        }),
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('action', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body', StructType([
            StructField('meter_stop', IntegerType(), True), 
            StructField('timestamp', StringType(), True), 
            StructField('transaction_id', IntegerType(), True), 
            StructField('reason', StringType(), True), 
            StructField('id_tag', StringType(), True), 
            StructField('transaction_data', ArrayType(StringType(), True), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.timestamp for x in result.collect()]
    expected_data = ['2022-01-01T08:20:00+00:00', '2022-01-01T09:20:00+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_stop_transaction_request_unpack_json_unit(spark, stop_transaction_request_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_stop_transaction_request_unpack_json_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body', StructType([
            StructField('meter_stop', IntegerType(), True), 
            StructField('timestamp', StringType(), True), 
            StructField('transaction_id', IntegerType(), True), 
            StructField('reason', StringType(), True), 
            StructField('id_tag', StringType(), True), 
            StructField('transaction_data', ArrayType(StringType(), True), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.timestamp for x in result.sort(col("new_body.timestamp")).limit(3).collect()]
    expected_data = ['2023-01-01T17:56:55.669396+00:00', '2023-01-01T18:31:34.833396+00:00', '2023-01-01T19:10:01.568021+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_stop_transaction_request_unpack_json_e2e(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

def stop_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten))

# COMMAND ----------

########### SOLUTION ############

def stop_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df.\
        withColumn("meter_stop", input_df.new_body.meter_stop).\
        withColumn("timestamp", input_df.new_body.timestamp).\
        withColumn("transaction_id", input_df.new_body.transaction_id).\
        withColumn("reason", input_df.new_body.reason).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn("transaction_data", input_df.new_body.transaction_data).\
        drop("new_body").\
        drop("body")
    ###

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

def test_stop_transaction_request_flatten_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "action": "StopTransaction",
            "message_type": 2,
            "charge_point_id": "123",
            "body": json.dumps({
                "meter_stop": 2780,
                "timestamp": "2022-01-01T08:20:00+00:00",
                "transaction_id": 1,
                "reason": None,
                "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
                "transaction_data": None
            }),
        },
        {
            "action": "StartTransaction",
            "message_type": 2,
            "charge_point_id": "123",
            "body": json.dumps({
                "meter_stop": 5000,
                "timestamp": "2022-01-01T09:20:00+00:00",
                "transaction_id": 1,
                "reason": None,
                "id_tag": "25b72fa9-85fd-4a75-acbe-5a15fc7430a8",
                "transaction_data": None
            }),
        },
    ])

    body_schema = StructType([
        StructField('meter_stop', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('reason', StringType(), True), 
        StructField('id_tag', StringType(), True), 
        StructField('transaction_data', ArrayType(StringType(), True), True)
    ])
    
    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    ).withColumn("new_body", from_json(col("body"), body_schema))
    
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('action', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('meter_stop', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('reason', StringType(), True), 
        StructField('id_tag', StringType(), True), 
        StructField('transaction_data', ArrayType(StringType(), True), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.collect()]
    expected_data = ['2022-01-01T08:20:00+00:00', '2022-01-01T09:20:00+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_stop_transaction_request_flatten_unit(spark, stop_transaction_request_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_stop_transaction_request_flatten_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('meter_stop', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('reason', StringType(), True), 
        StructField('id_tag', StringType(), True), 
        StructField('transaction_data', ArrayType(StringType(), True), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.sort(col("timestamp")).limit(3).collect()]
    expected_data = ['2023-01-01T17:56:55.669396+00:00', '2023-01-01T18:31:34.833396+00:00', '2023-01-01T19:10:01.568021+00:00']
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_stop_transaction_request_flatten_e2e(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process MeterValues Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Filter
# MAGIC In this exercise, filter for the `MeterValues` action and the "Request" (`2`) message_type.

# COMMAND ----------

def meter_values_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = None
    message_type = None
    ###
    return input_df.filter((input_df.action == "MeterValues") & (input_df.message_type == 2))

display(df.transform(meter_values_request_filter))

# COMMAND ----------

######### SOLUTION ###########

def meter_values_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = "MeterValues"
    message_type = 2
    ###
    return input_df.filter((input_df.action == "MeterValues") & (input_df.message_type == 2))

display(df.transform(meter_values_request_filter))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_meter_values_request_filter_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "MeterValues",
        "message_type": 2,
        "charge_point_id": "123"
    },
    {
        "action": "MeterValues",
        "message_type": 3,
        "charge_point_id": "123"
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_data = [ (x.action, x.message_type) for x in result.collect()]
    expected_data = [("MeterValues", 2)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    print("All tests pass! :)")
    
test_meter_values_request_filter_unit(spark, meter_values_request_filter)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd

def test_meter_values_request_filter_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    display(result)

    result_count = result.count()
    expected_count = 218471
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_action = [ x.action for x in result.select("action").distinct().collect()]
    expected_action = ["MeterValues"]
    assert result_action == expected_action, f"Expected {expected_action}, but got {result_action}"
    
    result_message_type = [ x.message_type for x in result.select("message_type").distinct().collect()]
    expected_message_type = [2]
    assert result_message_type == expected_message_type, f"Expected {expected_message_type}, but got {result_message_type}"

    print("All tests pass! :)")
    
test_meter_values_request_filter_e2e(df.transform(meter_values_request_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- meter_value: array (nullable = true)
# MAGIC  |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |-- timestamp: string (nullable = true)
# MAGIC  |    |    |    |-- sampled_value: array (nullable = true)
# MAGIC  |    |    |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |    |    |-- value: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- context: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- format: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- measurand: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- phase: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- unit: string (nullable = true)
# MAGIC ```

# COMMAND ----------

def meter_values_request_unpack_json(input_df: DataFrame):
    sampled_value_schema = StructType([
        StructField("value", StringType()),
        StructField("context", StringType()),
        StructField("format", StringType()),
        StructField("measurand", StringType()),
        StructField("phase", StringType()),
        StructField("unit", StringType()),
    ])

    meter_value_schema = StructType([
        StructField("timestamp", StringType()),
        StructField("sampled_value", ArrayType(sampled_value_schema)),
    ])

    body_schema = StructType([
        StructField("connector_id", IntegerType()),
        StructField("transaction_id", IntegerType()),
        StructField("meter_value", ArrayType(meter_value_schema)),
    ])
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json))

# COMMAND ----------

############## SOLUTION ##############

def meter_values_request_unpack_json(input_df: DataFrame):
    sampled_value_schema = StructType([
        StructField("value", StringType()),
        StructField("context", StringType()),
        StructField("format", StringType()),
        StructField("measurand", StringType()),
        StructField("phase", StringType()),
        StructField("unit", StringType()),
    ])

    meter_value_schema = StructType([
        StructField("timestamp", StringType()),
        StructField("sampled_value", ArrayType(sampled_value_schema)),
    ])

    body_schema = StructType([
        StructField("connector_id", IntegerType()),
        StructField("transaction_id", IntegerType()),
        StructField("meter_value", ArrayType(meter_value_schema)),
    ])
    ### YOUR CODE HERE
    return input_df.withColumn("new_body", from_json(col("body"), body_schema))
    ###

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_meter_values_request_unpack_json_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "MeterValues",
        "message_type": 2,
        "charge_point_id": "123",
        "body": '{"connector_id": 1, "meter_value": [{"timestamp": "2022-10-02T15:30:17.000345+00:00", "sampled_value": [{"value": "0.00", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L1-N", "location": "Outlet", "unit": "V"}, {"value": "13.17", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L1", "location": "Outlet", "unit": "A"}, {"value": "3663.49", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1", "location": "Outlet", "unit": "W"}, {"value": "238.65", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L2-N", "location": "Outlet", "unit": "V"}, {"value": "14.28", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L2", "location": "Outlet", "unit": "A"}, {"value": "3086.46", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L2", "location": "Outlet", "unit": "W"}, {"value": "215.21", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L3-N", "location": "Outlet", "unit": "V"}, {"value": "14.63", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L3", "location": "Outlet", "unit": "A"}, {"value": "4014.47", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L3", "location": "Outlet", "unit": "W"}, {"value": "254.65", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": null, "location": "Outlet", "unit": "Wh"}, {"value": "11.68", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L1-N", "location": "Outlet", "unit": "V"}, {"value": "3340.61", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L1", "location": "Outlet", "unit": "A"}, {"value": "7719.95", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1", "location": "Outlet", "unit": "W"}, {"value": "0.00", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L2-N", "location": "Outlet", "unit": "V"}, {"value": "3.72", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L2", "location": "Outlet", "unit": "A"}, {"value": "783.17", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L2", "location": "Outlet", "unit": "W"}, {"value": "242.41", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L3-N", "location": "Outlet", "unit": "V"}, {"value": "3.46", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L3", "location": "Outlet", "unit": "A"}, {"value": "931.52", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L3", "location": "Outlet", "unit": "W"}, {"value": "1330", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": null, "location": "Outlet", "unit": "W"},{"value": "7.26", "context": "Sample.Periodic", "format": "Raw", "measurand": "Energy.Active.Import.Register", "phase": null, "location": "Outlet", "unit": "Wh"}]}], "transaction_id": 1}'
    },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("body", StringType()),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('action', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body',StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_value", ArrayType(StructType([
                StructField("timestamp", StringType(), True),
                StructField("sampled_value", ArrayType(StructType([
                    StructField("value", StringType(), True),
                    StructField("context", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("measurand", StringType(), True),
                    StructField("phase", StringType(), True),
                    StructField("unit", StringType(), True)
                ]), True), True)
            ]), True), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.transaction_id for x in result.collect()]
    expected_data = [1]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_meter_values_request_unpack_json_unit(spark, meter_values_request_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_meter_values_request_unpack_json_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 218471
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('new_body',StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_value", ArrayType(StructType([
                StructField("timestamp", StringType(), True),
                StructField("sampled_value", ArrayType(StructType([
                    StructField("value", StringType(), True),
                    StructField("context", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("measurand", StringType(), True),
                    StructField("phase", StringType(), True),
                    StructField("unit", StringType(), True)
                ]), True), True)
            ]), True), True)
        ]), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.new_body.transaction_id for x in result.sort(col("message_id")).limit(3).collect()]
    expected_data = [2562, 2562, 2562]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_meter_values_request_unpack_json_e2e(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns! You might need to use [explode](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.explode.html?highlight=explode#pyspark.sql.functions.explode) for certain nested structures.
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  |-- measurand: string (nullable = true)
# MAGIC  |-- phase: string (nullable = true)
# MAGIC  |-- value: double (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import explode, to_timestamp, round
from pyspark.sql.types import DoubleType


def meter_values_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df
    ###

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json).transform(meter_values_request_flatten))

# COMMAND ----------

############ SOLUTION #############

from pyspark.sql.functions import explode, to_timestamp, round
from pyspark.sql.types import DoubleType


def meter_values_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    return input_df. \
        select("*", explode("new_body.meter_value").alias("meter_value")). \
        select("*", explode("meter_value.sampled_value").alias("sampled_value")). \
        withColumn("timestamp", to_timestamp(col("meter_value.timestamp"))).\
        withColumn("measurand", col("sampled_value.measurand")).\
        withColumn("phase", col("sampled_value.phase")).\
        withColumn("value", round(col("sampled_value.value").cast(DoubleType()),2)).\
        select("message_id", "message_type", "charge_point_id", "action", "write_timestamp", col("new_body.transaction_id").alias("transaction_id"), "timestamp", "measurand", "phase", "value")
    ###

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json).transform(meter_values_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from pyspark.sql.types import TimestampType
from datetime import datetime

def test_meter_values_request_flatten_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
    {
        "action": "MeterValues",
        "message_id": "f8635eee-dd37-4c80-97f9-6a4f1ad3a40b",
        "message_type": 2,
        "charge_point_id": "123",
        "write_timestamp": "2022-10-02T15:30:17.000345+00:00",
        "body": '{"connector_id": 1, "meter_value": [{"timestamp": "2022-10-02T15:30:17.000345+00:00", "sampled_value": [{"value": "0.00", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L1-N", "location": "Outlet", "unit": "V"}, {"value": "13.17", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L1", "location": "Outlet", "unit": "A"}, {"value": "3663.49", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1", "location": "Outlet", "unit": "W"}, {"value": "238.65", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L2-N", "location": "Outlet", "unit": "V"}, {"value": "14.28", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L2", "location": "Outlet", "unit": "A"}, {"value": "3086.46", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L2", "location": "Outlet", "unit": "W"}, {"value": "215.21", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L3-N", "location": "Outlet", "unit": "V"}, {"value": "14.63", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L3", "location": "Outlet", "unit": "A"}, {"value": "4014.47", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L3", "location": "Outlet", "unit": "W"}, {"value": "254.65", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": null, "location": "Outlet", "unit": "Wh"}, {"value": "11.68", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L1-N", "location": "Outlet", "unit": "V"}, {"value": "3340.61", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L1", "location": "Outlet", "unit": "A"}, {"value": "7719.95", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L1", "location": "Outlet", "unit": "W"}, {"value": "0.00", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L2-N", "location": "Outlet", "unit": "V"}, {"value": "3.72", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L2", "location": "Outlet", "unit": "A"}, {"value": "783.17", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L2", "location": "Outlet", "unit": "W"}, {"value": "242.41", "context": "Sample.Periodic", "format": "Raw", "measurand": "Voltage", "phase": "L3-N", "location": "Outlet", "unit": "V"}, {"value": "3.46", "context": "Sample.Periodic", "format": "Raw", "measurand": "Current.Import", "phase": "L3", "location": "Outlet", "unit": "A"}, {"value": "931.52", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": "L3", "location": "Outlet", "unit": "W"}, {"value": "1330", "context": "Sample.Periodic", "format": "Raw", "measurand": "Power.Active.Import", "phase": null, "location": "Outlet", "unit": "W"},{"value": "7.26", "context": "Sample.Periodic", "format": "Raw", "measurand": "Energy.Active.Import.Register", "phase": null, "location": "Outlet", "unit": "Wh"}]}], "transaction_id": 1}'
    },
    ])

    body_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_value", ArrayType(StructType([
                StructField("timestamp", StringType(), True),
                StructField("sampled_value", ArrayType(StructType([
                    StructField("value", StringType(), True),
                    StructField("context", StringType(), True),
                    StructField("format", StringType(), True),
                    StructField("measurand", StringType(), True),
                    StructField("phase", StringType(), True),
                    StructField("unit", StringType(), True)
                ]), True), True)
            ]), True), True)
        ])
    
    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("action", StringType()),
            StructField("message_id", StringType()),
            StructField("message_type", IntegerType()),
            StructField("charge_point_id", StringType()),
            StructField("write_timestamp", StringType()),
            StructField("body", StringType()),
        ])
    ).withColumn("new_body", from_json(col("body"), body_schema))
    
    result = input_df.transform(f)
    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 21
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('timestamp', TimestampType(), True), 
        StructField('measurand', StringType(), True), 
        StructField('phase', StringType(), True), 
        StructField('value', DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.limit(2).collect()]
    expected_data = [datetime(2022, 10, 2, 15, 30, 17, 345), datetime(2022, 10, 2, 15, 30, 17, 345)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_meter_values_request_flatten_unit(spark, meter_values_request_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json

def test_meter_values_request_flatten_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2621652
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('message_id', StringType(), True), 
        StructField('message_type', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('timestamp', TimestampType(), True), 
        StructField('measurand', StringType(), True), 
        StructField('phase', StringType(), True), 
        StructField('value', DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    result_data = [ x.timestamp for x in result.sort(col("timestamp")).limit(3).collect()]
    expected_data = [datetime(2023, 1, 1, 10, 43, 15, 900215), datetime(2023, 1, 1, 10, 43, 15, 900215), datetime(2023, 1, 1, 10, 43, 15, 900215)]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"


    print("All tests pass! :)")
    
test_meter_values_request_flatten_e2e(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json).transform(meter_values_request_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Request to Parquet
# MAGIC In this exercise, write the StartTransaction Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_start_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionRequest"
    ### YOUR CODE HERE
    input_df
    ###
    

write_start_transaction_request(df.\
    transform(start_transaction_request_filter).\
    transform(start_transaction_request_unpack_json).\
    transform(start_transaction_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionRequest")))

# COMMAND ----------

############ SOLUTION ##############

def write_start_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionRequest"
    ### YOUR CODE HERE
    input_df.\
        write.\
        mode("overwrite").\
        parquet(output_directory)
    ###
    

write_start_transaction_request(df.\
    transform(start_transaction_request_filter).\
    transform(start_transaction_request_unpack_json).\
    transform(start_transaction_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_write_start_transaction_request():
    df = spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionRequest"))
    df.show()
    snappy_parquet_count = df.filter(col("name").endswith(".snappy.parquet")).count()
    assert snappy_parquet_count == 1, f"Expected 1 .snappy.parquet file, but got {snappy_parquet_count}"
    
    success_count = df.filter(col("name") == "_SUCCESS").count()
    assert success_count == 1, f"Expected 1 _SUCCESS file, but got {success_count}"
    
    print("All tests pass! :)")
    
test_write_start_transaction_request()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Response to Parquet
# MAGIC In this exercise, write the StartTransaction Response data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_start_transaction_response(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionResponse"
    ### YOUR CODE HERE
    input_df
    ###

write_start_transaction_response(df.\
    transform(start_transaction_response_filter).\
    transform(start_transaction_response_unpack_json).\
    transform(start_transaction_response_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionResponse")))

# COMMAND ----------

############ SOLUTION ##############

def write_start_transaction_response(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionResponse"
    ### YOUR CODE HERE
    input_df.\
        write.\
        mode("overwrite").\
        parquet(output_directory)
    ###

write_start_transaction_response(df.\
    transform(start_transaction_response_filter).\
    transform(start_transaction_response_unpack_json).\
    transform(start_transaction_response_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionResponse")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_write_start_transaction_response():
    df = spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionResponse"))
    df.show()
    snappy_parquet_count = df.filter(col("name").endswith(".snappy.parquet")).count()
    assert snappy_parquet_count == 1, f"Expected 1 .snappy.parquet file, but got {snappy_parquet_count}"
    
    success_count = df.filter(col("name") == "_SUCCESS").count()
    assert success_count == 1, f"Expected 1 _SUCCESS file, but got {success_count}"
    
    print("All tests pass! :)")
    
test_write_start_transaction_response()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StopTransaction Request to Parquet
# MAGIC In this exercise, write the StopTransaction Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_stop_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StopTransactionRequest"
    ### YOUR CODE HERE
    input_df
    ###

write_stop_transaction_request(df.\
    transform(stop_transaction_request_filter).\
    transform(stop_transaction_request_unpack_json).\
    transform(stop_transaction_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StopTransactionRequest")))

# COMMAND ----------

############ SOLUTION ##############

def write_stop_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StopTransactionRequest"
    ### YOUR CODE HERE
    input_df.\
        write.\
        mode("overwrite").\
        parquet(output_directory)
    ###

write_stop_transaction_request(df.\
    transform(stop_transaction_request_filter).\
    transform(stop_transaction_request_unpack_json).\
    transform(stop_transaction_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StopTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_write_stop_transaction_request():
    df = spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StopTransactionRequest"))
    df.show()
    snappy_parquet_count = df.filter(col("name").endswith(".snappy.parquet")).count()
    assert snappy_parquet_count == 1, f"Expected 1 .snappy.parquet file, but got {snappy_parquet_count}"
    
    success_count = df.filter(col("name") == "_SUCCESS").count()
    assert success_count == 1, f"Expected 1 _SUCCESS file, but got {success_count}"
    
    print("All tests pass! :)")
    
test_write_stop_transaction_request()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write MeterValues Request to Parquet
# MAGIC In this exercise, write the MeterValues Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_meter_values_request(input_df: DataFrame):
    output_directory = f"{out_dir}/MeterValuesRequest"
    ### YOUR CODE HERE
    input_df
    ###

write_meter_values_request(df.\
    transform(meter_values_request_filter).\
    transform(meter_values_request_unpack_json).\
    transform(meter_values_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/MeterValuesRequest")))

# COMMAND ----------

############ SOLUTION ##############

def write_meter_values_request(input_df: DataFrame):
    output_directory = f"{out_dir}/MeterValuesRequest"
    ### YOUR CODE HERE
    input_df.\
        write.\
        mode("overwrite").\
        parquet(output_directory)
    ###

write_meter_values_request(df.\
    transform(meter_values_request_filter).\
    transform(meter_values_request_unpack_json).\
    transform(meter_values_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/MeterValuesRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_write_meter_values_request():
    df = spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/MeterValuesRequest"))
    df.show()
    snappy_parquet_count = df.filter(col("name").endswith(".snappy.parquet")).count()
    assert snappy_parquet_count == 1, f"Expected 1 .snappy.parquet file, but got {snappy_parquet_count}"
    
    success_count = df.filter(col("name") == "_SUCCESS").count()
    assert success_count == 1, f"Expected 1 _SUCCESS file, but got {success_count}"
    
    print("All tests pass! :)")
    
test_write_meter_values_request()

# COMMAND ----------

df.select("action").distinct().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congrats for finishing the Batch Processing Silver Tier exercise! We now have unpacked and flattened data for:
# MAGIC * StartTransaction Request
# MAGIC * StartTransaction Response
# MAGIC * StopTransaction Request
# MAGIC * MeterValues Request
# MAGIC 
# MAGIC Hypothetically, we could have also done the same for the remaining actions (e.g. Heartbeat Request/Response, BootNotification Request/Response), but to save some time, we've only processed the actions that are relevant to the Gold layers that we'll build next (thin-slices, ftw!). You might have noticed that some of the processing steps were a bit repetitive and especially towards the end, could definitely be D.R.Y.'ed up (and would be in production code), but for the purposes of the exercise, we've gone the long route.

# COMMAND ----------


