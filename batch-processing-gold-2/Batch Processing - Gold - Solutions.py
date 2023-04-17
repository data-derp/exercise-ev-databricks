# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Gold
# MAGIC 
# MAGIC Remember our domain question, **What is the final charge time and final charge dispense for every completed transaction**? It was the exercise which required several joins and window queries. :)  We're here to do it again (the lightweight version) but with the help of the work we did in the Silver Tier. 
# MAGIC 
# MAGIC Steps:
# MAGIC * Match StartTransaction Requests and Responses
# MAGIC * Join Stop Transaction Requests and StartTransaction Responses, matching on transaction_id (left join)
# MAGIC * Find the matching StartTransaction Requests (left join)
# MAGIC * Calculate the total_time (withColumn, cast, maths)
# MAGIC * Calculate total_energy (withColumn, cast)
# MAGIC * Calculate total_parking_time (explode, filter, window, groupBy)
# MAGIC * Join and Shape (left join, select) 
# MAGIC 
# MAGIC **NOTE:** You've already done these these exercises before. We absolutely recommond bringing over your answers from that exercise to speed things along (with some minor tweaks), because you already know how to do all of that already! Of course, you're welcome to freshly rewrite your answers to test yourself!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "batch_processing_gold"

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
# MAGIC ## Read Data from Silver Layer
# MAGIC Let's read the parquet files that we created in the bronze layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_silver")
print(input_dir)


# COMMAND ----------

dbutils.fs.ls(f"{input_dir}/output")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
start_transaction_request_df = read_parquet(f"{input_dir}/output/StartTransactionRequest")
start_transaction_response_df = read_parquet(f"{input_dir}/output/StartTransactionResponse")
stop_transaction_request_df = read_parquet(f"{input_dir}/output/StopTransactionRequest")
meter_values_request_df = read_parquet(f"{input_dir}/output/MeterValuesRequest")

display(start_transaction_request_df)
display(start_transaction_response_df)
display(stop_transaction_request_df)
display(meter_values_request_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Match StartTransaction Requests and Responses
# MAGIC In this exercise, match StartTransaction Requests and Responses using a [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) on `message_id`.
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC ```

# COMMAND ----------

def match_start_transaction_requests_with_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    start_transaction_response_df
display(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df))

# COMMAND ----------

########## SOLUTION ##########
def match_start_transaction_requests_with_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.\
        join(join_df, input_df.message_id == join_df.message_id, "left").\
        select(
            input_df.charge_point_id.alias("charge_point_id"), 
            input_df.transaction_id.alias("transaction_id"), 
            join_df.meter_start.alias("meter_start"), 
            join_df.timestamp.alias("start_timestamp")
        )
    ###
    start_transaction_response_df
display(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from typing import Callable
import pandas as pd
import json
from pyspark.sql.functions import from_json, col

def test_match_start_transaction_requests_with_responses_unit(spark, f: Callable):
    input_start_transaction_response_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "message_id": "456",
            "transaction_id": 1,
            "id_tag_info_status": "Accepted",
            "id_tag_info_parent_id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "id_tag_info_expiry_date": None
        }
    ])

    input_start_transaction_response_df = spark.createDataFrame(
        input_start_transaction_response_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("message_id", StringType()),
            StructField("transaction_id", IntegerType()),
            StructField("id_tag_info_status", StringType(), True),
            StructField("id_tag_info_parent_id_tag", StringType(), True),
            StructField("id_tag_info_expiry_date", StringType(), True),
        ])
    )

    input_start_transaction_request_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "message_id": "456",
            "connector_id": 1,
            "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "meter_start": 0,
            "timestamp": "2022-01-01T08:00:00+00:00",
            "reservation_id": None
        },
    ])

    input_start_transaction_request_df = spark.createDataFrame(
        input_start_transaction_request_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("message_id", StringType()),
            StructField("connector_id", IntegerType(), True),
            StructField("id_tag", StringType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("reservation_id", IntegerType(), True),
        ])
    )

    result = input_start_transaction_response_df.transform(f, input_start_transaction_request_df)

    print("Transformed DF:")
    result.show()

    result_count = result.count()
    assert result_count == 1
    result_schema = result.schema
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True),
        StructField('transaction_id', IntegerType(), True),
        StructField('meter_start', IntegerType(), True),
        StructField('start_timestamp', StringType(), True),
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")
    
test_match_start_transaction_requests_with_responses_unit(spark, match_start_transaction_requests_with_responses)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_match_start_transaction_requests_with_responses_e2e(input_df, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF")
    result.show()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('meter_start', IntegerType(), True), 
        StructField('start_timestamp', StringType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    result_data = [x.transaction_id for x in result.sort(col("transaction_id")).limit(3).collect()]
    expected_data = [1, 2, 3]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    print("All tests pass! :)")

test_match_start_transaction_requests_with_responses_e2e(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df), spark, display)



# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Stop Transaction Requests and StartTransaction Responses
# MAGIC In this exercise, [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) Stop Transaction Requests and the newly joined StartTransaction Request/Response DataFrame (from the previous exercise), matching on transaction_id (left join).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  ```

# COMMAND ----------

def join_with_start_transaction_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
        ### YOUR CODE HERE
        return input_df
        ###

    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    )
)

# COMMAND ----------

############## SOLUTION ##############
def join_with_start_transaction_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
        ### YOUR CODE HERE
        return input_df. \
        join(join_df, input_df.transaction_id == join_df.transaction_id, "left"). \
        select(
            join_df.charge_point_id, 
            join_df.transaction_id, 
            join_df.meter_start, 
            input_df.meter_stop.alias("meter_stop"), 
            join_df.start_timestamp, 
            input_df.timestamp.alias("stop_timestamp")
        )
        ###

    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from pyspark.sql import Row
from typing import Any

def test_join_with_start_transaction_responses_unit(spark, f: Callable):
    input_start_transaction_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "transaction_id": 1,
            "meter_start": 0,
            "start_timestamp":  "2022-01-01T08:00:00+00:00"
        },
    ])

    input_start_transaction_df = spark.createDataFrame(
        input_start_transaction_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("transaction_id", IntegerType()),
            StructField("meter_start", IntegerType()),
            StructField("start_timestamp", StringType()),
        ])
    )

    input_stop_transaction_request_pandas = pd.DataFrame([
        {
            "foo": "bar",
            "meter_stop": 2780,
            "timestamp": "2022-01-01T08:20:00+00:00",
            "transaction_id": 1,
            "reason": None,
            "id_tag": "ea068c10-1bfb-4128-ab88-de565bd5f02f",
            "transaction_data": None
        }
    ])

    input_stop_transaction_request_schema = StructType([
        StructField("foo", StringType(), True),
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", StringType(), True),
    ])

    input_stop_transaction_request_df = spark.createDataFrame(
        input_stop_transaction_request_pandas,
        input_stop_transaction_request_schema
    )


    result = input_stop_transaction_request_df.transform(f, input_start_transaction_df)

    print("Transformed DF:")
    result.show()

    result_count = result.count()
    assert result_count == 1

    result_row = result.collect()[0]
    def assert_row_value(row: Row, field: str, value: Any):
        r = getattr(row, field)
        assert getattr(row, field) == value, f"Expected {value} but got {r}"

    assert_row_value(result_row, "charge_point_id", "123")
    assert_row_value(result_row, "transaction_id", 1)
    assert_row_value(result_row, "meter_start", 0)
    assert_row_value(result_row, "meter_stop", 2780)
    assert_row_value(result_row, "start_timestamp", "2022-01-01T08:00:00+00:00")
    assert_row_value(result_row, "stop_timestamp", "2022-01-01T08:20:00+00:00")

    print("All tests pass! :)")

test_join_with_start_transaction_responses_unit(spark, join_with_start_transaction_responses)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from typing import List, Any

def test_join_with_start_transaction_responses_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF:")
    display_f(result)

    assert set(result.columns) == {"charge_point_id", "transaction_id", "meter_start", "meter_stop", "start_timestamp", "stop_timestamp"}
    assert result.count() == 2599, f"expected 95, but got {result.count()}"

    result_sub = result.sort(col("transaction_id")).limit(3)
    print("Reordered DF under test:")
    display_f(result_sub)

    def assert_expected_value(column: str, expected_values: List[Any]):
        values = [getattr(x, column) for x in result_sub.select(col(column)).collect()]
        assert values == expected_values, f"expected {expected_values} in column {column}, but got {values}"

    assert_expected_value("charge_point_id", ['94073806-8222-430e-8ca4-fab78b58fb67', 'acea7af6-eb97-4158-8549-2edda4aab255', '7e8404de-845e-4562-9587-720707e87de8'])
    assert_expected_value("transaction_id", [1, 2, 3])
    assert_expected_value("meter_start", [0, 0, 0])
    assert_expected_value("meter_stop", [95306, 78106, 149223])
    assert_expected_value("start_timestamp", ['2023-01-01T10:43:09.900215+00:00', '2023-01-01T11:20:31.296429+00:00', '2023-01-01T14:03:42.294160+00:00'])
    assert_expected_value("stop_timestamp", ['2023-01-01T18:31:34.833396+00:00', '2023-01-01T17:56:55.669396+00:00', '2023-01-01T23:19:26.063351+00:00'])

    print("All tests pass! :)")

test_join_with_start_transaction_responses_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ),
    spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate the total_time
# MAGIC Using Pyspark functions [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html) and [cast](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast) and little creative maths, calculate the total charging time (stop_timestamp - start_timestamp) in hours (two decimal places).
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_time(input_df: DataFrame) -> DataFrame:
    seconds_in_one_hour = 3600
    ### YOUR CODE HERE
    return input_df
    ###
    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)



# COMMAND ----------

############# SOLUTION ###############
from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_time(input_df: DataFrame) -> DataFrame:
    seconds_in_one_hour = 3600
    ### YOUR CODE HERE
    return input_df. \
        withColumn("total_time", col("stop_timestamp").cast("long")/seconds_in_one_hour - col("start_timestamp").cast("long")/seconds_in_one_hour). \
        withColumn("total_time", round(col("total_time").cast(DoubleType()),2))
    ###
    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import TimestampType
from pandas import Timestamp
from typing import Callable
import pandas as pd

def test_calculate_total_time_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": '123',
            "transaction_id": 1,
            "meter_start": 0,
            "meter_stop": 1000,
            "start_timestamp": datetime.fromisoformat("2023-01-01T08:00:00+00:00"),
            "stop_timestamp": datetime.fromisoformat("2023-01-01T09:00:00+00:00"),
        }
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("meter_stop", IntegerType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("stop_timestamp", TimestampType(), True),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF:")
    result.show()
    result.printSchema()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"

    result_values = result.toPandas().to_dict(orient="records")
    expected_values = [{
        "charge_point_id": '123',
        "transaction_id": 1,
        "meter_start": 0,
        "meter_stop": 1000,
        "start_timestamp": Timestamp('2023-01-01 08:00:00.000000'),
        "stop_timestamp": Timestamp('2023-01-01 09:00:00.000000'),
        "total_time": 1.0
    }]
    assert result_values == expected_values, f"expected {expected_values}, but got {result_values}"

    result_total_time = [x.total_time for x in result.collect()]
    expected_total_time = [1.0]
    assert result_total_time == expected_total_time, f"expected {expected_total_time}, but got {result_total_time}"

    print("All tests pass! :)")

test_calculate_total_time_unit(spark, calculate_total_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_calculate_total_time_hours_e2e(input_df: DataFrame, **kwargs):
    result = input_df
    print("Transformed DF:")
    result.show()
    result.printSchema()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('meter_start', IntegerType(), True), 
        StructField('meter_stop', IntegerType(), True), 
        StructField('start_timestamp', TimestampType(), True), 
        StructField('stop_timestamp', TimestampType(), True), 
        StructField('total_time', DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"

    result_total_time = [x.total_time for x in result.sort(col("transaction_id")).limit(3).collect()]
    expected_total_time = [7.81, 6.61, 9.26]
    
    assert result_total_time == expected_total_time, f"expected {expected_total_time}, but got {result_total_time}"
    print("All tests passed! :)")

test_calculate_total_time_hours_e2e(
    stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_energy
# MAGIC Calculate total_energy (withColumn, cast)
# MAGIC Using [withColumn](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn) and [cast](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast), calculate the total energy by subtracting `meter_stop` from `meter_start` and rounding to the nearest 2 decimal points.
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

############ SOLUTION ############

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df \
        .withColumn("total_energy", col("meter_stop") - col("meter_start")) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    ###

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

def test_calculate_total_energy_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": '123',
            "transaction_id": 1,
            "meter_start": 0,
            "meter_stop": 1000,
            "start_timestamp": Timestamp('2023-01-01 08:00:00.000000'),
            "stop_timestamp": Timestamp('2023-01-01 09:00:00.000000'),
            "total_time": 1.0
        }
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("meter_stop", IntegerType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("stop_timestamp", TimestampType(), True),
            StructField("total_time", DoubleType(), True),
        ])
    )

    result = input_df.transform(f)
    print("Transformed DF:")
    result.show()
    result.printSchema()

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"

    result_values = result.toPandas().to_dict(orient="records")
    expected_values = [{
        "charge_point_id": '123',
        "transaction_id": 1,
        "meter_start": 0,
        "meter_stop": 1000,
        "start_timestamp": Timestamp('2023-01-01 08:00:00.000000'),
        "stop_timestamp": Timestamp('2023-01-01 09:00:00.000000'),
        "total_time": 1.0,
        "total_energy": 1000.0
    }]
    assert result_values == expected_values, f"expected {expected_values}, but got {result_values}"

    result_total_energy = [x.total_energy for x in result.collect()]
    expect_total_energy = [1000.0]
    assert result_total_energy == expect_total_energy, f"expected {expect_total_energy}, but got {result_total_energy}"

    print("All tests pass! :)")

test_calculate_total_energy_unit(spark, calculate_total_energy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_calculate_total_energy_e2e(input_df: DataFrame, spark, display_f, **kwargs):
    result = input_df

    print("Transformed DF:")
    result.show()
    result.printSchema()

    result_count = result.count()
    expected_count = 2599
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("meter_stop", IntegerType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("stop_timestamp", TimestampType(), True),
        StructField("total_time", DoubleType(), True),
        StructField("total_energy", DoubleType(), True),
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    result_ordered = result.sort(col("transaction_id"))
    result_total_energy = [x.total_energy for x in result_ordered.limit(3).collect()]
    expected_total_energy = [95306.0, 78106.0, 149223.0]
    assert result_total_energy == expected_total_energy, f"expected {expected_total_energy}, but got {result_total_energy}"
    print("All tests passed! :)")

test_calculate_total_energy_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_parking_time
# MAGIC In our target object, there is a field `total_parking_time` which is the number of hours that the EV is plugged in but not charging. This denoted in the **Meter Values Request** by the `measurand` = `Power.Active.Import` where `phase` is `None` or `null` and a value of `0`.
# MAGIC 
# MAGIC While it might seem easy on the surface, the logic is actually quite complex and will require you to spend some time understanding [Windows](https://sparkbyexamples.com/pyspark/pyspark-window-functions/) in order for you to complete it. Don't worry, take your time to think through the problem!
# MAGIC 
# MAGIC We'll need to do this in a handful of steps:
# MAGIC 1. Build a DataFrame from our MeterValue Request data with `transaction_id`, `timestamp`, `measurand`, `phase`, and `value` pulled out as columns (explode)
# MAGIC  2. Return only rows with `measurand` = `Power.Active.Import` and `phase` = `Null`
# MAGIC 3. Figure out how to represent in the DataFrame when a Charger is actively charging or not charging, calculate the duration of each of those groups, and sum the duration of the non charging groups as the `total_parking_time`
# MAGIC 
# MAGIC **Notes**
# MAGIC * There may be many solutions but the focus should be on using the Spark built-in API
# MAGIC * You should be able to accomplish this entirely in DataFrames without for-expressions
# MAGIC * You'll use the following functions
# MAGIC   * [explode](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.explode.html?highlight=explode#pyspark.sql.functions.explode)
# MAGIC   * [filter](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)
# MAGIC   * [Window](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Window.html?highlight=window#pyspark.sql.Window)
# MAGIC   * [groupBy](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html?highlight=groupby#pyspark.sql.DataFrame.groupBy)
# MAGIC * This is the same exercise we completed in the past - please feel free to use your solution there to speed things up unless you'd like to try to solve it on your own again.
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window

def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    window_by_transaction = Window.partitionBy("transaction_id").orderBy(col("timestamp").asc())
    window_by_transaction_group = Window.partitionBy(["transaction_id", "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ### YOUR CODE HERE
    return input_df
    ###

display(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time)
)

# COMMAND ----------

############## SOLUTION ###############
from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window

def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    window_by_transaction = Window.partitionBy("transaction_id").orderBy(col("timestamp").asc())
    window_by_transaction_group = Window.partitionBy(["transaction_id", "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ### YOUR CODE HERE
    return input_df.\
        withColumn("charging", when(col("value") > 0,1).otherwise(0)).\
        withColumn("boundary", abs(col("charging")-lag(col("charging"), 1, 0).over(window_by_transaction))).\
        withColumn("charging_group", sum("boundary").over(window_by_transaction)).\
        select(col("transaction_id"), "timestamp", "value", "charging", "boundary", "charging_group").\
        withColumn("first", first('timestamp').over(window_by_transaction_group).alias("first_id")).\
        withColumn("last", last('timestamp').over(window_by_transaction_group).alias("last_id")).\
        filter(col("charging") == 0).\
        groupBy("transaction_id", "charging_group").agg(
            first((col("last").cast("long") - col("first").cast("long"))).alias("group_duration")
        ).\
        groupBy("transaction_id").agg(
            round((sum(col("group_duration"))/3600).cast(DoubleType()), 2).alias("total_parking_time")
        )
    ###

display(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from dateutil import parser
from typing import List, Any

def test_calculate_total_parking_time_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 1.0,
            "timestamp": parser.parse("2023-01-01T09:00:00Z")
        },
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 1.0,
            "timestamp": parser.parse("2023-01-01T09:05:00Z")
        },
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 1.0,
            "timestamp": parser.parse("2023-01-01T09:10:00Z")
        },
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:15:00Z")
        },
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:20:00Z")
        },
        {
            "transaction_id": 1,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:25:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 2.0,
            "timestamp": parser.parse("2023-01-01T09:00:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 2.0,
            "timestamp": parser.parse("2023-01-01T09:05:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 2.0,
            "timestamp": parser.parse("2023-01-01T09:10:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:15:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:20:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:25:00Z")
        },
        {
            "transaction_id": 2,
            "measurand": "Power.Active.Import",
            "phase": None,
            "value": 0.0,
            "timestamp": parser.parse("2023-01-01T09:30:00Z")
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("measurand", StringType(), True),
            StructField("phase", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
    )

    print("Transformed DF:")
    result = input_df.transform(f)
    result.show()

    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    def assert_expected_value(df: DataFrame, column_name: str, expected_values: List[Any]):
        r = [getattr(x, column_name) for x in df.collect()]
        assert r == expected_values, f"Expected {expected_values}, but got {r}"

    assert_expected_value(result, "transaction_id", [1, 2])
    assert_expected_value(result, "total_parking_time", [0.17, 0.25])

    result_schema = result.schema
    expected_schema = StructType([
        StructField('transaction_id', IntegerType(), True),
        StructField('total_parking_time', DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    print("All tests pass! :)")

test_calculate_total_parking_time_unit(spark, calculate_total_parking_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_calculate_total_parking_time_e2e(input_df: DataFrame, spark, display_f: Callable, **kwargs):
    result = input_df

    display_f(result)

    result_count = result.count()
    expected_count = 2583
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType([
        StructField('transaction_id', IntegerType(), True),
        StructField('total_parking_time', DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    result_total_parking_time = [x.total_parking_time for x in result.limit(3).collect()]
    expected_total_parking_time = [2.17, 1.58, 1.25]
    assert result_total_parking_time == expected_total_parking_time, f"Expected {expected_total_parking_time}, but got {result_total_parking_time}"

    print("All tests pass! :)")

test_calculate_total_parking_time_e2e(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time), spark, display)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join and Shape
# MAGIC 
# MAGIC Join and Shape (left join, select)
# MAGIC 
# MAGIC Now that we have the `total_parking_time`, we can join that with our Target Dataframe (where we stored our Stop/Start Transaction data).
# MAGIC 
# MAGIC Recall that our newly transformed DataFrame has the following schema:
# MAGIC ```
# MAGIC root
# MAGIC |-- transaction_id: integer (nullable = true)
# MAGIC |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC  
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

def join_and_shape(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------

########### SOLUTION ############
def join_and_shape(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.\
        join(joined_df, on=input_df.transaction_id == joined_df.transaction_id, how="left").\
        select(
            input_df.charge_point_id, 
            input_df.transaction_id, 
            input_df.meter_start, 
            input_df.meter_stop, 
            input_df.start_timestamp, 
            input_df.stop_timestamp, 
            input_df.total_time, 
            input_df.total_energy, 
            joined_df.total_parking_time
        )
    ###

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

def test_join_and_shape_unit(spark, f: Callable):
    join_df_pandas = pd.DataFrame([
        {
            "transaction_id": 1,
            "total_parking_time": 0.1
        },
        {
            "transaction_id": 3,
            "total_parking_time": 0.2
        }
    ])

    join_df = spark.createDataFrame(
        join_df_pandas,
        StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("total_parking_time", DoubleType(), True),
        ])
    )

    input_df_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "transaction_id": 1,
            "meter_start": 0,
            "meter_stop": 100,
            "start_timestamp": parser.parse("2023-01-01T09:00:00Z"),
            "stop_timestamp": parser.parse("2023-01-01T09:30:00Z"),
            "total_time": 0.5,
            "total_energy": 100,
        },
        {
            "charge_point_id": "123",
            "transaction_id": 2,
            "meter_start": 0,
            "meter_stop": 200,
            "start_timestamp": parser.parse("2023-01-01T09:00:00Z"),
            "stop_timestamp": parser.parse("2023-01-01T09:30:00Z"),
            "total_time": 0.5,
            "total_energy": 200,
        },
        {
            "charge_point_id": "123",
            "transaction_id": 3,
            "meter_start": 0,
            "meter_stop": 300,
            "start_timestamp": parser.parse("2023-01-01T09:00:00Z"),
            "stop_timestamp": parser.parse("2023-01-01T09:30:00Z"),
            "total_time": 0.5,
            "total_energy": 300,
        }
    ])
    input_df = spark.createDataFrame(
        input_df_pandas,
        StructType([
            StructField("charge_point_id", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("meter_stop", IntegerType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("stop_timestamp", TimestampType(), True),
            StructField("total_time", DoubleType(), True),
            StructField("total_energy", DoubleType(), True),
        ])
    )

    result = input_df.transform(f, join_df)
    print("Transformed DF:")
    result.show()

    result_count = result.count()
    expected_count = 3
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("meter_stop", IntegerType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("stop_timestamp", TimestampType(), True),
        StructField("total_time", DoubleType(), True),
        StructField("total_energy", DoubleType(), True),
        StructField("total_parking_time", DoubleType(), True),
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    result_total_parking_time = [x.total_parking_time for x in result.collect()]
    expected_total_parking_time = [0.1, None, 0.2]
    assert result_total_parking_time == expected_total_parking_time, f"Expected {expected_total_parking_time}, but got {result_total_parking_time}"

    print("All tests pass! :)")

test_join_and_shape_unit(spark, join_and_shape)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

def test_join_and_shape_df_e2e(input_df: DataFrame, spark, display_f: Callable, **kwargs):
    result = input_df

    display_f(result)

    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("meter_stop", IntegerType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("stop_timestamp", TimestampType(), True),
        StructField("total_time", DoubleType(), True),
        StructField("total_energy", DoubleType(), True),
        StructField("total_parking_time", DoubleType(), True),
    ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    result_total_parking_time = [x.total_parking_time for x in result.limit(3).collect()]
    expected_total_parking_time = [2.17, 1.58, 1.0]
    assert result_total_parking_time == expected_total_parking_time, f"Expected {expected_total_parking_time}, but got {result_total_parking_time}"

    print("All tests pass!")

test_join_and_shape_df_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     ), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congratulations on finishing the Gold Tier exercise! Compared to a previous exercise where we did some of these exact exercises, you might have noticed that this time around, it was significantly easier to comprehend and complete because we didn't need to perform as many repetitive transformations to get to the interesting business logic.
# MAGIC 
# MAGIC * What might you do with this data now that you've transformed it?

# COMMAND ----------


