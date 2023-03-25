# Databricks notebook source
# MAGIC %md
# MAGIC # Final Charge Time and Charge Dispensed for Completed Charges
# MAGIC A CDR (Charge Data Record) is an important piece of information required by a CPO to invoice a customer for the charge dispensed in a transaction. A CDR contains information like:
# MAGIC 
# MAGIC | CDR field | Description |
# MAGIC | --- | --- |
# MAGIC | total_energy | How much charge was dispensed (StopTransactionRequest.meter_stop - StartTransactionRequest.meter_start) |
# MAGIC | total_time |  How long the transaction was (StopTransactionRequest.timestamp - StartTransactionRequest.timestamp) | 
# MAGIC | total_parking_time |  Total time of the transaction - time spent charging (because charging can be paused in the middle of a transaction) | 
# MAGIC | charging_periods |  The chunks of time spend actually charging (in reality, we'd group this additionlly by different electricity tariffs (e.g. one day tariff, one night tariff), but let's ignore cost for now) | 
# MAGIC 
# MAGIC 
# MAGIC We can calculate this from our OCPP Event data. After the Charge Point has registered itself with the CSMS (Charging Station Management System), it sends information via the OCPP protocol about the Transactions, in the following order:
# MAGIC 
# MAGIC | OCPP Action | OCPP Message Type | Description | Payload |
# MAGIC | --- | --- | --- | -- |
# MAGIC | StartTransaction | Request | Event sent when a Transaction that has been initiated by the car (or by itself on a scheduled basis). This payload contains the start timestamp (`timestamp`) of the charge and the meter reading (`meter_start`) at the time of the event. This does not contain a transaction ID (but the response back to the Charge Point does). | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/StartTransactionRequest.json)
# MAGIC | StartTransaction | Response | A response sent back from the Central System to the Charge Point upon receiving a StartTransaction request. This payload contains a transaction ID. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/StartTransactionResponse.json)
# MAGIC | MeterValues | Request | Message sent at a set frequency (configured per Charge Point) until the Transaction has ended that samples energy throughput at various outlets. Measurand `Energy.Active.Import.Register` gives a cumulative reading of the charge that has been dispensed for the transaction. Measurand `Power.Active.Import` gives the instantaneous charge at the time of reading. This data contains a transaction ID. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/MeterValuesRequest.json) |
# MAGIC | MeterValues | Response | A response sent back from the Central System to the Charge Point upon receiving a MeterValues request. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/MeterValuesResponse.json) |
# MAGIC | StopTransaction | Request | Event sent when the car has stopped a Transaction. It contains a transaction ID, the stop timestamp of the charge, and the meter reading (`meter_stop`) at the time of the event. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/StopTransactionRequest.json) |
# MAGIC | StopTransaction | Response | A response sent back from the Central System to the Charge Point upon receiving a MeterValues request. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/StopTransactionResponse.json) |
# MAGIC 
# MAGIC 
# MAGIC In this exercise, we'll inspect the historial data that we have and calculate the total charge per Charge Point for all completed transactions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "final_charge_time_charge_dispensed_completed_charges"

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
# MAGIC ## THINKING AHEAD

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Final Shape of Data
# MAGIC Before we start to ingest our data, it's helpful to know in what direction we're going. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contents
# MAGIC * Data Ingestion
# MAGIC   * Read OCPP Data (data.csv)
# MAGIC   * Read Transactions Data (transactions.csv)
# MAGIC * Data Transformation
# MAGIC   * Return only StopTransaction (filter)
# MAGIC   * Unpack JSON in StopTransaction (from_json)
# MAGIC   * Flattening your Data (select)
# MAGIC   * Join Transactions with Stop Transaction Records that exist for those transactions (inner join)
# MAGIC   * Rename timestamp column to "stop_timestamp" (withColumnRenamed)
# MAGIC   * Convert the start_timestamp and stop_timestamp fields to timestamp type (to_timestamp, withColumn)
# MAGIC   * Calculate the Charge Session Duration (withColumn, cast, round, mathops)
# MAGIC   * Cleanup extra columns (select)
# MAGIC   * Unpack JSON in StopTransaction (from_json)
# MAGIC   * Flatten MeterValues JSON (select, explode, alias)
# MAGIC   * Most recent Energy.Active.Import.Register Reading (filter, to_timestamp, window, order by)
# MAGIC   * Cast Value to double (cast)
# MAGIC   * All together now! (left join)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Read OCPP Data

# COMMAND ----------

# MAGIC %md
# MAGIC In the last exercise, we already learned how to read in data (and we did it!). Let's read it in again, but this time, it's already filled out. Run the next two cells below to read in our OCPP data.

# COMMAND ----------

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1679654583.csv"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        StructField("message_id", StringType(), True),
        StructField("message_type", IntegerType(), True),
        StructField("charge_point_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("write_timestamp", StringType(), True),
        StructField("body", StringType(), True),
    ])
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("escape", "\\") \
        .schema(custom_schema) \
        .load(filepath)
    return df
    
df = create_dataframe(filepath)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC We also have a new dataset to pull in, called "transactions". While the `StopTransaction` and `MeterValues` payloads contain a `transaction_id`, the `StartTransaction` payload does not. There is a separate `Transactions` dataset which maps between the `charge_point_id`, an `id_tag` (an RFID card which is optional to authorize a charge), a `timestamp` of the start of the transaction, and a `transaction_id`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Return only StopTransaction Requests
# MAGIC Recall that we need to calculate the charge time and amount of charged dispensed for stopped transactions. Before we do that, we need a dataframe that only has StopTransaction data. Use the [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) method to return only data where the `action == "StopTransaction"` and `message_type == 2`.

# COMMAND ----------

def return_stop_transaction_requests(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
stop_transaction_df = df.transform(return_stop_transaction_requests)
display(stop_transaction_df)

# COMMAND ----------

######## SOLUTION ########
def return_stop_transaction_requests(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.filter((input_df.action == "StopTransaction") & (input_df.message_type == 2))
    ###
    
stop_transaction_request_df = df.transform(return_stop_transaction_requests)
display(stop_transaction_request_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test! 
# MAGIC 
# MAGIC **NOTE:** Inspect carefully what the unit test actually tests: it creates a DataFrame with mock data and calls only the function that it should be testing. For the purposes of this exercise, this is the only in-line unit test (for demonstrative purposes); the remainder of the tests are hidden as to not spoil the solutions of the exercise itself.

# COMMAND ----------

import pandas as pd

def test_return_stop_transaction_unit():
    input_pandas = pd.DataFrame([
        {
            "foo": "30e2ed0c-dd61-4fc1-bcb8-f0a8a0f87c0a",
            "message_type": 2,
            "action": "bar",
        },
        {
            "foo": "4496309f-dfc5-403d-a1c1-54d21b9093c1",
            "message_type": 2,
            "action": "StopTransaction",
        },
        {
            "foo": "bb7b2cd0-f140-4ffe-8280-dc462784303d",
            "message_type": 2,
            "action": "zebra",
        }

    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("foo", StringType()),
            StructField("message_type", IntegerType()),
            StructField("action", StringType()),
        ])
    )

    result = input_df.transform(return_stop_transaction_requests)
    result_count = result.count()
    assert result_count == 1, f"expected 1, but got {result_count}"

    result_actions = [x.action for x in result.collect()]
    expected_actions = ["StopTransaction"]
    assert result_actions == expected_actions, f"expect {expected_actions}, but got {result_actions}"

    result_message_type = [x.message_type for x in result.collect()]
    expected_message_type = [2]
    assert result_message_type == expected_message_type, f"expect {expected_message_type}, but got {result_message_type}"

    print("All tests pass! :)")
    
test_return_stop_transaction_unit()

# COMMAND ----------

# MAGIC %md
# MAGIC And now the test to ensure that our real data is transformed the way we want.

# COMMAND ----------

def test_return_stoptransaction():
    result = df.transform(return_stop_transaction_requests)
    
    count =  result.count()
    expected_count = 95
    assert count == expected_count, f"expected {expected_count} got {count}"
    
    unique_actions = set([ x["action"] for x in result.select("action").collect()])
    expected_actions = set(["StopTransaction"])
    assert unique_actions == expected_actions, f"expected {expected_actions}, but got {unique_actions}"
    
    print("All tests pass! :)")
    
test_return_stoptransaction()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Unpack JSON in StopTransaction
# MAGIC Note in the current schema of the StopTransaction Dataframe we just created, the body is a string (containing JSON):

# COMMAND ----------

stop_transaction_request_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC That body contains the majority of the important information, including the `transaction_id`, for which we need as a starting point to fetch all other data related to the transaction. Unfortunately, we can't query this string for `transaction_id`:

# COMMAND ----------

from pyspark.sql.functions import col

stop_transaction_request_df.select(col("body.transaction_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC What happened here? We can't select `"body.transaction_id"` if it is a `string` type.  But we CAN if it is read in as JSON. 
# MAGIC 
# MAGIC For reference, there are a [variety of ways of handling JSON in a Dataframe](https://sparkbyexamples.com/pyspark/pyspark-json-functions-with-examples/).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Your turn!
# MAGIC Unpack the `body` column (currently a JSON string) for just the `StopTransaction` messages into a new column called `new_body` using the `with_column` and `from_json` functions and the following schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- meter_stop: string (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, ArrayType, DoubleType, LongType


def stop_transaction_body_schema():
    return StructType([
        ### YOUR CODE HERE
        
        ###
    ])
    
def convert_stop_transaction_request_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

    display(df.transform(return_stoptransaction).transform(convert_stop_transaction_request_json))


# COMMAND ----------

########### SOLUTION ############
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, ArrayType, DoubleType, LongType

def stop_transaction_body_schema():
    return StructType([
        ### YOUR CODE HERE
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
        ###
    ])
    
def convert_stop_transaction_request_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), stop_transaction_body_schema()))
    ###

display(df.transform(return_stop_transaction_requests).transform(convert_stop_transaction_request_json))


# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_stop_transaction_unit

test_convert_stop_transaction_unit(spark, convert_stop_transaction_request_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the E2E test!

# COMMAND ----------

def test_convert_stop_transaction_json():
    result = df.transform(return_stop_transaction_requests).transform(convert_stop_transaction_request_json)
    
    print("Transformed DF:")
    result.show()
    
    assert result.columns == ["message_id", "message_type", "charge_point_id", "action", "write_timestamp", "body", "new_body"]
    assert result.count() == 95, f"expected 95, but got {result.count()}"
    
    result_sub = result.limit(3)
    
    meter_stop = [x.meter_stop for x in result_sub.select(col("new_body.meter_stop")).collect()]
    expected_meter_stop = [51219, 31374, 50781]
    assert meter_stop == expected_meter_stop, f"expected {expected_meter_stop}, but got {meter_stop}"
    
    timestamps = [x.timestamp for x in result_sub.select(col("new_body.timestamp")).collect()]
    expected_timestamps = ['2023-01-01T17:11:31.399112+00:00', '2023-01-01T17:48:30.073819+00:00', '2023-01-01T20:57:10.917742+00:00']
    assert timestamps == expected_timestamps, f"expected {expected_timestamps}, but got {timestamps}"
    
    transaction_ids = [x.transaction_id for x in result_sub.select(col("new_body.transaction_id")).collect()]
    expected_transaction_ids = [1, 5, 7]
    assert transaction_ids == expected_transaction_ids, f"expected {expected_transaction_ids}, but got {transaction_ids}"
    
    reasons = [x.reason for x in result_sub.select(col("new_body.reason")).collect()]
    expected_reasons = [None, None, None]
    assert reasons == expected_reasons, f"expected {expected_reasons}, but got {reasons}"
    
    id_tags = [x.id_tag for x in result_sub.select(col("new_body.id_tag")).collect()]
    expected_id_tags = ['e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5']
    assert id_tags == expected_id_tags, f"expected {expected_id_tags}, but got {id_tags}"
    
    transaction_data = [x.transaction_data for x in result_sub.select(col("new_body.transaction_data")).collect()]
    expected_transaction_data = [None, None, None]
    assert transaction_data == expected_transaction_data, f"expected {expected_transaction_data}, but got {transaction_data}"
    
    print("All tests pass! :)")

    
test_convert_stop_transaction_json()

# COMMAND ----------

# MAGIC %md 
# MAGIC So, can we now query the json?

# COMMAND ----------

from pyspark.sql.functions import col

stop_transaction_json_df = df.transform(return_stop_transaction_requests).transform(convert_stop_transaction_request_json)
stop_transaction_json_df.select(col("new_body.transaction_id")).show(5)

# COMMAND ----------

stop_transaction_json_df.new_body.transaction_id

# COMMAND ----------

# MAGIC %md
# MAGIC Yes we can! :)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Unpack StartTransaction Response
# MAGIC Using the `transaction_id` in the StopTransaction Requests, we can start to build out our target object by using the `transaction_id` to find the related StartTransaction Response, and then the related StartTransaction Request. Why the weird route? The StartTransaction Response has a `transaction_id` and the StartTransaction Request doesn't; but the StartTransaction Request has valuable information, namely `meter_start` and a `timestamp` value we can use as our `start_timestamp`. The StartTransaction Request and Response both have the same `message_id` by design so we can use that to locate the relevant records. We won't traverse this route until later, but it's important understand where we need to be.
# MAGIC 
# MAGIC Very similarly to the previous exercise, we need to unpack the StartTransaction Response `body` column from a json string to json so we can eventually join our data on the `transaction_id` column.
# MAGIC 
# MAGIC The schema of the resulting JSON should be as follows:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- id_tag_info: struct  (nullable = true)
# MAGIC       |-- status: string  (nullable = true)
# MAGIC       |-- parent_id_tag: string  (nullable = true)
# MAGIC       |-- expiry_date: string  (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType

def start_transaction_response_body_schema():

    ### YOUR CODE HERE
    schema = None
    ###

    return schema
    
    
def convert_start_transaction_response_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###


display(df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json))


# COMMAND ----------

########### SOLUTION ############
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType

def start_transaction_response_body_schema():

    ### YOUR CODE HERE
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])
    ###

    return schema
    
    
def convert_start_transaction_response_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), start_transaction_response_body_schema()))
    ###


display(df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json))



# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_response_json_unit

test_convert_start_transaction_response_json_unit(spark, convert_start_transaction_response_json)


# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E!

# COMMAND ----------

def test_convert_start_transaction_response_json():
    result = df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json)
    
    print("Transformed DF:")
    display(result)
    
    assert result.columns == ["message_id", "message_type", "charge_point_id", "action", "write_timestamp", "body", "new_body"]
    assert result.count() == 95, f"expected 95, but got {result.count()}"
    
    result_sub = result.limit(3)

    def assert_expected_json_value(json_path: str, expected_values: List[Any]):
        values = [getattr(x, json_path.split(".")[-1]) for x in result_sub.select(col(json_path)).collect()]
        assert values == expected_values, f"expected {expected_values}, but got {values}"
    
    assert_expected_json_value("new_body.transaction_id", [1, 2, 3])
    assert_expected_json_value("new_body.id_tag_info.status", ['Accepted', 'Accepted', 'Accepted'])
    assert_expected_json_value("new_body.id_tag_info.parent_id_tag", ['e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5'])
    assert_expected_json_value("new_body.id_tag_info.expiry_date", [None, None, None])
    
    print("All tests pass! :)")

    
test_convert_start_transaction_response_json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Unpack StartTransaction Request
# MAGIC We know we'll need to extract a couple of fields from the StartTransaction Request events, namely `meter_start` and `timestamp` (eventually our `start_timestamp` in the target object).
# MAGIC 
# MAGIC As we have done with the StopTransaction Request and the StartTransaction Response, unpack the StartTransaction Request `body` column (currently a json string) into a new JSON column called `new_body` with the following schema:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: string  (nullable = true)
# MAGIC  |-- meter_start: integer  (nullable = true)
# MAGIC  |-- timestamp: string  (nullable = true)
# MAGIC  |-- reservation_id: string  (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType

def start_transaction_request_body_schema():

    ### YOUR CODE HERE
    schema = None
    ###

    return schema
    
    
def convert_start_transaction_request_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###


display(df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json))



# COMMAND ----------

########### SOLUTION ############
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType

def start_transaction_request_body_schema():

    ### YOUR CODE HERE
    schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    ###

    return schema
    
    
def convert_start_transaction_request_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("new_body",from_json(col("body"), start_transaction_request_body_schema()))
    ###


display(df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json))



# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_response_json_unit

test_convert_start_transaction_request_unit(spark, convert_start_transaction_request_json)


# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

from typing import Any, List

def test_convert_start_transaction_request_json():
    result = df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json)
    
    print("Transformed DF:")
    display(result)
    
    assert result.columns == ["message_id", "message_type", "charge_point_id", "action", "write_timestamp", "body", "new_body"]
    assert result.count() == 95, f"expected 95, but got {result.count()}"
    
    result_sub = result.limit(3)

    def assert_expected_json_value(json_path: str, expected_values: List[Any]):
        values = [getattr(x, json_path.split(".")[-1]) for x in result_sub.select(col(json_path)).collect()]
        assert values == expected_values, f"expected {expected_values}, but got {values}"
    
    assert_expected_json_value("new_body.connector_id", [1, 2, 1])
    assert_expected_json_value("new_body.id_tag", ['e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5', 'e812abe5-e73b-453d-b71d-29ef6e1593f5'])
    assert_expected_json_value("new_body.meter_start", [0, 0, 0])
    assert_expected_json_value("new_body.timestamp", ['2023-01-01T12:54:04.750286+00:00', '2023-01-01T12:57:35.483812+00:00', '2023-01-01T13:48:12.471750+00:00'])
    assert_expected_json_value("new_body.reservation_id", [None, None, None])
    
    print("All tests pass! :)")

    
test_convert_start_transaction_request_json()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Find the matching StartTransaction Requests (Left Join)
# MAGIC Now that we have unpacked the events for StartTransaction Request and StartTransaction Response, we can find our matching StartTransaction Request for each StartTransaction Response by executing a **left** [join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) between the StartTransaction Response and the StartTransaction Request on the column `message_id`. 
# MAGIC 
# MAGIC Make sure to return the following columns using the [select](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.select.html?highlight=select) function:
# MAGIC * charge_point_id
# MAGIC * transaction_id
# MAGIC * meter_start
# MAGIC * start_timestamp (the `timestamp` column from StartTransaction Request)

# COMMAND ----------

def join_with_start_transaction_request(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

start_transaction_response_df = df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json)
start_transaction_request_df = df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json)

start_transaction_df = start_transaction_response_df.transform(join_with_start_transaction_request, start_transaction_request_df)
display(start_transaction_df)

# COMMAND ----------

########### SOLUTION ############

def join_with_start_transaction_request(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.join(join_df, input_df.message_id == join_df.message_id, "left").select(input_df.charge_point_id.alias("charge_point_id"), input_df.new_body.transaction_id.alias("transaction_id"), join_df.new_body.meter_start.alias("meter_start"), join_df.new_body.timestamp.alias("start_timestamp"))
    ###

start_transaction_response_df = df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json)
start_transaction_request_df = df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json)

start_transaction_df = start_transaction_response_df.transform(join_with_start_transaction_request, start_transaction_request_df)
display(start_transaction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_with_start_transaction_request_unit

test_join_with_start_transaction_request_unit(spark, join_with_start_transaction_request)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

def test_join_with_start_transaction_request():
    result = df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json). \
    transform(join_with_start_transaction_request, df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json))
    
    print("Transformed DF:")
    display(result)
    
    assert result.columns == ["charge_point_id", "transaction_id", "meter_start", "start_timestamp"]
    assert result.count() == 95, f"expected 95, but got {result.count()}"
    
    result_sub = result.limit(3)
    
    def assert_expected_value(column: str, expected_values: List[Any]):
        values = [getattr(x, column) for x in result_sub.select(col(column)).collect()]
        assert values == expected_values, f"expected {expected_values}, but got {values}"
    assert_expected_value("charge_point_id", ['01a0f039-7685-4a7f-9ef6-8d262a7898fb', '3e365f3f-6e30-43d3-b897-d6291a9f7c35', '77b7feb3-7f8f-4faf-86c6-d725e70e8c7f'])
    assert_expected_value("transaction_id", [1, 2, 3])
    assert_expected_value("meter_start", [0, 0, 0])
    assert_expected_value("start_timestamp",  ['2023-01-01T13:48:12.471750+00:00', '2023-01-04T18:46:22.322745+00:00', '2023-01-04T19:57:40.882560+00:00'])
    
    print("All tests pass! :)")

    
test_join_with_start_transaction_request()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Start and Stop Data
# MAGIC Now that we have our StartTransaction events joined together, we can now join our DataFrame with the StopTransaction Request data that contains `meter_stop` and `timestamp`.
# MAGIC 
# MAGIC Executing a **left** [join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) between StopTransaction Request and the StartTransaction DataFrame on the column `new_body.transaction_id`. Make sure to return the following columns using the [select](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.select.html?highlight=select) function:
# MAGIC * charge_point_id
# MAGIC * transaction_id
# MAGIC * meter_start
# MAGIC * meter_stop
# MAGIC * start_timestamp
# MAGIC * stop_timestamp (the `timestamp` column from StopTransaction Request)
# MAGIC 
# MAGIC #### Reflect
# MAGIC Do we join the StartTransaction DataFrame to our StopTransaction Request data or vice versa?

# COMMAND ----------

def join_stop_with_start(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
    return input_df
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json)))
display(result)

# COMMAND ----------

########### SOLUTION ############

def join_stop_with_start(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
    return input_df.join(join_df, input_df.new_body.transaction_id == join_df.transaction_id, "left").select(join_df.charge_point_id, join_df.transaction_id, join_df.meter_start, input_df.new_body.meter_stop.alias("meter_stop"), join_df.start_timestamp, input_df.new_body.timestamp.alias("stop_timestamp"))
    ###

result = df.transform(return_stop_transaction_requests). \
transform(convert_stop_transaction_request_json). \
transform(
    join_stop_with_start, 
    df.filter(
        (df.action == "StartTransaction") & (df.message_type == 3)). \
        transform(convert_start_transaction_response_json).\
        transform(
            join_with_start_transaction_request, 
            df.filter(
                (df.action == "StartTransaction") & (df.message_type == 2)
            ).transform(convert_start_transaction_request_json)))
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------


from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_stop_with_start_unit

test_join_stop_with_start_unit(spark, join_stop_with_start)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

def test_join_stop_with_start_unit():
    result = df.transform(return_stop_transaction_requests). \
        transform(convert_stop_transaction_request_json). \
        transform(
            join_stop_with_start, 
            df.filter(
                (df.action == "StartTransaction") & (df.message_type == 3)). \
                transform(convert_start_transaction_response_json).\
                transform(
                    join_with_start_transaction_request, 
                    df.filter(
                        (df.action == "StartTransaction") & (df.message_type == 2)
                    ).transform(convert_start_transaction_request_json)))
    
    print("Transformed DF:")
    display(result)
    
    assert set(result.columns) == set(["charge_point_id", "transaction_id", "meter_start", "meter_stop", "start_timestamp", "stop_timestamp"])
    assert result.count() == 95, f"expected 95, but got {result.count()}"
    
    result_sub = result.limit(3)
    
    def assert_expected_value(column: str, expected_values: List[Any]):
        values = [getattr(x, column) for x in result_sub.select(col(column)).collect()]
        assert values == expected_values, f"expected {expected_values}, but got {values}"

    assert_expected_value("charge_point_id", ['01a0f039-7685-4a7f-9ef6-8d262a7898fb', '7af0d94b-e864-4ffd-9c30-8970831f3870', 'c2e32e4a-4387-4cd4-bb40-dde977bc56b1'])
    assert_expected_value("transaction_id", [1, 5, 7])
    assert_expected_value("meter_start", [0, 0, 0])
    assert_expected_value("meter_start", [0, 0, 0])
    assert_expected_value("start_timestamp",  ['2023-01-01T12:54:04.750286+00:00', '2023-01-01T15:20:29.693922+00:00', '2023-01-01T17:48:01.776488+00:00'])
    assert_expected_value("stop_timestamp",  ['2023-01-01T17:11:31.399112+00:00', '2023-01-01T17:48:30.073819+00:00', '2023-01-01T20:57:10.917742+00:00'])
    
    print("All tests pass! :)")

    
test_join_stop_with_start_unit()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Convert the start_timestamp and stop_timestamp fields to timestamp type
# MAGIC At some point soon, we'll need to calculate the time in hours between the `start_timestamp` and `stop_timestamp` columns. However, note that both columns are of type `string`
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  ```
# MAGIC  
# MAGIC  In this exercise, we'll convert the `start_timestamp` and `stop_timestamp` columns to a timestamp type.
# MAGIC  
# MAGIC  Target schema:
# MAGIC  ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def convert_start_stop_timestamp_to_timestamp_type(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type)
display(result)

# COMMAND ----------

############ SOLUION #############
from pyspark.sql.functions import to_timestamp

def convert_start_stop_timestamp_to_timestamp_type(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("start_timestamp", to_timestamp("start_timestamp")).withColumn("stop_timestamp", to_timestamp("stop_timestamp"))
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type)

result.printSchema


# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_stop_timestamp_to_timestamp_type_unit

test_convert_start_stop_timestamp_to_timestamp_type_unit(spark, convert_start_stop_timestamp_to_timestamp_type)


# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

from pyspark.sql.types import TimestampType
def test_convert_start_stop_timestamp_to_timestamp_type():
    result = df.transform(return_stop_transaction_requests). \
        transform(convert_stop_transaction_request_json). \
        transform(
            join_stop_with_start, 
            df.filter(
                (df.action == "StartTransaction") & (df.message_type == 3)). \
                transform(convert_start_transaction_response_json).\
                transform(
                    join_with_start_transaction_request, 
                    df.filter(
                        (df.action == "StartTransaction") & (df.message_type == 2)
                    ).transform(convert_start_transaction_request_json))). \
        transform(convert_start_stop_timestamp_to_timestamp_type)
    
    result_count = result.count()
    expected_count = 95
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True), 
        StructField("transaction_id", IntegerType(), True), 
        StructField("meter_start", IntegerType(), True), 
        StructField("meter_stop", IntegerType(), True), 
        StructField("start_timestamp", TimestampType(), True), 
        StructField("stop_timestamp", TimestampType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    print("All tests passed! :)")
    
test_convert_start_stop_timestamp_to_timestamp_type()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate the Charge Transaction Duration (total_time)
# MAGIC Now that we have our `start_timestamp` and `stop_timestamp` columns in the appropriate type, we now can calculate the time in hours of the charge by subtracting the `start_timestamp` from the `stop_timestamp` column and doing some arithmetic.
# MAGIC 
# MAGIC Current schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC Expected schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true) 
# MAGIC ```
# MAGIC 
# MAGIC Round to two decimal places (note: it might not appear as two decimal places in the resulting Dataframe as a result of rendering).
# MAGIC 
# MAGIC **Hint**: You can convert a timestamp type to seconds using the [cast](...) function and the `long` type. Of course, that's just seconds. :bulb:

# COMMAND ----------

from pyspark.sql.functions import round

def calculate_total_time_hours(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type). \
    transform(calculate_total_time_hours)

display(result)

# COMMAND ----------

######### SOLUTION ##########
from pyspark.sql.functions import round

def calculate_total_time_hours(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    seconds_in_one_hour = 3600
    return input_df \
        .withColumn("total_time", col("stop_timestamp").cast("long")/seconds_in_one_hour - col("start_timestamp").cast("long")/seconds_in_one_hour) \
        .withColumn("total_time", round(col("total_time").cast(DoubleType()),2))
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type). \
    transform(calculate_total_time_hours)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_time_hours_unit

test_calculate_total_time_hours_unit(spark, calculate_total_time_hours)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E!

# COMMAND ----------

from pyspark.sql.types import TimestampType

def test_calculate_charge_duration_minutes():
    result = df.transform(return_stop_transaction_requests). \
        transform(convert_stop_transaction_request_json). \
        transform(
            join_stop_with_start, 
            df.filter(
                (df.action == "StartTransaction") & (df.message_type == 3)). \
                transform(convert_start_transaction_response_json).\
                transform(
                    join_with_start_transaction_request, 
                    df.filter(
                        (df.action == "StartTransaction") & (df.message_type == 2)
                    ).transform(convert_start_transaction_request_json))). \
        transform(convert_start_stop_timestamp_to_timestamp_type). \
        transform(calculate_total_time_hours)
    print("Transformed DF:")
    result.show()
    result.printSchema()
    
    result_count = result.count()
    expected_count = 95
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True), 
        StructField("transaction_id", IntegerType(), True), 
        StructField("meter_start", IntegerType(), True), 
        StructField("meter_stop", IntegerType(), True), 
        StructField("start_timestamp", TimestampType(), True), 
        StructField("stop_timestamp", TimestampType(), True), 
        StructField("total_time", DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    result_total_time = [x.total_time for x in result.sort(col("transaction_id")).collect()]
    expected_total_time = [4.29, 9.39, 7.89, 7.5, 2.47, 10.84, 3.15, 2.68, 6.03, 2.03, 4.03, 4.18, 4.64, 2.7, 9.21, 7.05, 10.5, 8.55, 8.9, 11.95, 11.38, 10.25, 3.55, 3.82, 9.17, 6.19, 6.28, 11.35, 4.18, 11.92, 2.16, 7.88, 8.44, 4.75, 7.14, 6.52, 5.76, 11.11, 9.44, 8.61, 2.7, 5.2, 8.04, 3.19, 3.37, 11.94, 10.39, 10.9, 2.02, 2.56, 10.33, 6.94, 4.88, 7.81, 5.56, 4.21, 2.97, 11.87, 9.16, 3.24, 7.23, 6.97, 11.86, 6.41, 5.96, 7.4, 9.02, 10.28, 4.87, 5.46, 10.53, 7.68, 10.93, 6.84, 7.09, 4.94, 10.84, 5.81, 5.36, 8.9, 5.56, 9.05, 2.48, 2.58, 2.91, 8.91, 8.87, 3.51, 10.82, 7.03, 8.92, 5.93, 2.03, 2.96, 2.28]
    assert result_total_time == expected_total_time, f"expected {expected_total_time}, but got {result_total_time}"
    print("All tests passed! :)")
    
test_calculate_charge_duration_minutes()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_energy
# MAGIC Let's add a column for the `total_energy`. Subtract `meter_start` from `meter_stop`.
# MAGIC 
# MAGIC Current schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true) 
# MAGIC ```
# MAGIC 
# MAGIC Expected schema:
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
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import round

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type). \
    transform(calculate_total_time_hours). \
    transform(calculate_total_energy)

display(result)

# COMMAND ----------

######### SOLUTION ##########
from pyspark.sql.functions import round

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df \
        .withColumn("total_energy", col("meter_stop") - col("meter_start")) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    ###

result = df.transform(return_stop_transaction_requests). \
    transform(convert_stop_transaction_request_json). \
    transform(
        join_stop_with_start, 
        df.filter(
            (df.action == "StartTransaction") & (df.message_type == 3)). \
            transform(convert_start_transaction_response_json).\
            transform(
                join_with_start_transaction_request, 
                df.filter(
                    (df.action == "StartTransaction") & (df.message_type == 2)
                ).transform(convert_start_transaction_request_json))). \
    transform(convert_start_stop_timestamp_to_timestamp_type). \
    transform(calculate_total_time_hours). \
    transform(calculate_total_energy)

display(result)

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_energy_unit

test_calculate_total_energy_unit(spark, calculate_total_energy)

# COMMAND ----------

from pyspark.sql.types import TimestampType

def test_calculate_total_energy():
    result = df.transform(return_stop_transaction_requests). \
        transform(convert_stop_transaction_request_json). \
        transform(
            join_stop_with_start, 
            df.filter(
                (df.action == "StartTransaction") & (df.message_type == 3)). \
                transform(convert_start_transaction_response_json).\
                transform(
                    join_with_start_transaction_request, 
                    df.filter(
                        (df.action == "StartTransaction") & (df.message_type == 2)
                    ).transform(convert_start_transaction_request_json))). \
        transform(convert_start_stop_timestamp_to_timestamp_type). \
        transform(calculate_total_time_hours).\
        transform(calculate_total_energy)

    print("Transformed DF:")
    result.show()
    result.printSchema()
    
    result_count = result.count()
    expected_count = 95
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
    result_total_energy = [x.total_energy for x in result_ordered.collect()]
    expected_total_energy = [51219.0, 146616.0, 151794.0, 106126.0, 31374.0, 193968.0, 50781.0, 42121.0, 95634.0, 23897.0, 43316.0, 43746.0, 77118.0, 34277.0, 144768.0, 98641.0, 170171.0, 137738.0, 149056.0, 199170.0, 227549.0, 117548.0, 42235.0, 48498.0, 145084.0, 83495.0, 76078.0, 174636.0, 74102.0, 177470.0, 25978.0, 144815.0, 105303.0, 86140.0, 133118.0, 102056.0, 92845.0, 176318.0, 136581.0, 155487.0, 36414.0, 96265.0, 125985.0, 37903.0, 52334.0, 211115.0, 182410.0, 157962.0, 21851.0, 23476.0, 164136.0, 95713.0, 86874.0, 104892.0, 75476.0, 60495.0, 47719.0, 229061.0, 128245.0, 43527.0, 94194.0, 112741.0, 210995.0, 98534.0, 98066.0, 116117.0, 147795.0, 147573.0, 62259.0, 73185.0, 197632.0, 127848.0, 172165.0, 74999.0, 105432.0, 78858.0, 198323.0, 101860.0, 73797.0, 145058.0, 83244.0, 151649.0, 29350.0, 33778.0, 38108.0, 123547.0, 149542.0, 37542.0, 160941.0, 95735.0, 158472.0, 91462.0, 25614.0, 29244.0, 25278.0]
    assert result_total_energy == expected_total_energy, f"expected {expected_total_energy}, but got {result_total_energy}"
    print("All tests passed! :)")
    
test_calculate_total_energy()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Unpack JSON in MeterValues
# MAGIC The last piece of data that we need to add to this DataFrame is the amount of charge dispensed. This piece of data comes from the MeterValues action where the `measurand` is `Energy.Active.Import.Register`. The JSON is actually quite nested ([example](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/metervalues.json)) with a schema of:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- body: struct (nullable = true)
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
# MAGIC 
# MAGIC We'll need to do something very similar to what we've done for `StopTransaction` already:
# MAGIC * convert the `body` to proper JSON
# MAGIC * flatten
# MAGIC 
# MAGIC In this exercise, we'll convert the `body` column to proper JSON (we'll tackle flattening in the next exercise) using the `from_json` function.
# MAGIC 
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
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
# MAGIC  ```
# MAGIC 
# MAGIC Let's quickly look at the MeterValues data that we have:

# COMMAND ----------

display(df.filter(df.action == "MeterValues"))


# COMMAND ----------

def convert_metervalues_to_json(input_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
    sampled_value_schema = StructType([
        None
    ])

    meter_value_schema = StructType([
        None
    ])

    body_schema = StructType([
        None
    ])
    
    return input_df
    ###

df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).printSchema()

# COMMAND ----------

########## SOLUTION ###########
def convert_metervalues_to_json(input_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
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
    
    return input_df.withColumn("new_body", from_json(col("body"), body_schema))
    ###

df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).printSchema()

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_metervalues_to_json_unit

test_convert_metervalues_to_json_unit(spark, convert_metervalues_to_json)

# COMMAND ----------

def test_convert_metervalues_to_json():
    result = df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json)
    print("Transformed DF:")
    result.show()
    result.printSchema()
    
    result_count = result.count()
    expected_count = 404
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("charge_point_id", StringType(), True), 
        StructField("write_timestamp", StringType(), True), 
        StructField("action", StringType(), True), 
        StructField("body", StringType(), True), 
        StructField("new_body", StructType([
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
                        StructField("unit", StringType(), True)]), True), True)]), True), True)]), True)])

    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"

    print("All tests passed! :)")
    
test_convert_metervalues_to_json()

# COMMAND ----------

from pyspark.sql.types import TimestampType
def test_get_most_recent_energy_active_import_register():
    result = df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).transform(get_most_recent_energy_active_import_register)
    
    result.show(10)
    result.printSchema()
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
            StructField("charge_point_id", StringType(), True), 
            StructField("action", StringType(), True), 
            StructField("write_timestamp", StringType(), True), 
            StructField("connector_id", IntegerType(), True), 
            StructField("transaction_id", IntegerType(), True), 
            StructField("value", StringType(), True), 
            StructField("context", StringType(), True), 
            StructField("format", StringType(), True), 
            StructField("phase", StringType(), True), 
            StructField("measurand", StringType(), True), 
            StructField("unit", StringType(), True), 
            StructField("timestamp", TimestampType(), True)
        ])
        
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    result_value = [ x.value for x in result.collect() ]
    expected_value = ["79.20", "99.59", "142.81", "45.14", "137.19", "115.28", "75.09", "142.16", "103.34", "21.60", "103.51", "135.95", "148.62", "82.16", "160.31", "142.01", "142.02", "27.84", "43.08", "157.34", "150.96", "82.72", "136.40", "30.85", "35.16", "40.18", "70.84"]
    assert result_value == expected_value, f"expected {expected_value}, but got {result_value}"
    
    result_measurand = set([ x.measurand for x in result.collect() ])
    expected_measurand = set(["Energy.Active.Import.Register"])
    assert result_measurand == expected_measurand, f"expected {expected_measurand}, but got {result_measurand}"
    
    print("All tests passed! :)")

test_get_most_recent_energy_active_import_register()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Cast value to Double
# MAGIC Notice that the `value` column a string value right now. Since we know we'll be generating some visualisations (or handing this data off to someone else) later, it makes sense to convert that value from a string to a Double.
# MAGIC 
# MAGIC Current schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- value: string (nullable = true)              // <- this one!
# MAGIC  |-- context: string (nullable = true)
# MAGIC  |-- format: string (nullable = true)
# MAGIC  |-- phase: string (nullable = true)
# MAGIC  |-- measurand: string (nullable = true)
# MAGIC  |-- unit: string (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  ```
# MAGIC 
# MAGIC Use the `withColumn` and `cast` functions to convert the `value` column to a Double Type.
# MAGIC 
# MAGIC Target schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- value: double (nullable = true)
# MAGIC  |-- context: string (nullable = true)
# MAGIC  |-- format: string (nullable = true)
# MAGIC  |-- phase: string (nullable = true)
# MAGIC  |-- measurand: string (nullable = true)
# MAGIC  |-- unit: string (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  ```
# MAGIC  ```

# COMMAND ----------

def cast_value_to_double(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).transform(get_most_recent_energy_active_import_register).transform(cast_value_to_double).printSchema()

# COMMAND ----------

########## SOLUTION ##########

def cast_value_to_double(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("value", col("value").cast("double"))
    ###

df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).transform(get_most_recent_energy_active_import_register).transform(cast_value_to_double).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_cast_value_to_double_unit

test_cast_value_to_double_unit(spark, cast_value_to_double)

# COMMAND ----------

from pyspark.sql.types import TimestampType

def test_cast_value_to_double():
    result = df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).transform(get_most_recent_energy_active_import_register).transform(cast_value_to_double)
    result.show(5)
    result.printSchema()
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
            StructField("charge_point_id", StringType(), True), 
            StructField("action", StringType(), True), 
            StructField("write_timestamp", StringType(), True), 
            StructField("connector_id", IntegerType(), True), 
            StructField("transaction_id", IntegerType(), True), 
            StructField("value", DoubleType(), True), 
            StructField("context", StringType(), True), 
            StructField("format", StringType(), True), 
            StructField("phase", StringType(), True), 
            StructField("measurand", StringType(), True), 
            StructField("unit", StringType(), True), 
            StructField("timestamp", TimestampType(), True)
        ])
        
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    result_value = [ x.value for x in result.collect() ]
    expected_value = [79.2, 99.59, 142.81, 45.14, 137.19, 115.28, 75.09, 142.16, 103.34, 21.6, 103.51, 135.95, 148.62, 82.16, 160.31, 142.01, 142.02, 27.84, 43.08, 157.34, 150.96, 82.72, 136.4, 30.85, 35.16, 40.18, 70.84]
    assert result_value == expected_value, f"expected {expected_value}, but got {result_value}"
        
    print("All tests passed! :)")
   
test_cast_value_to_double()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: All together now!
# MAGIC We took a few turns and side-quests but our last side-quest left us with a MeterValues DataFrame that we can now join to the DataFrame that we joined between transactions and StopTransactions. Schema below:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- charge_duration_minutes: double (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC All that's missing is to now join that DataFrame with the MeterValues DataFrame that we just curated and return a DataFrame that we can use to make some beautiful visualisations.
# MAGIC 
# MAGIC In this exercise, we will LEFT join the existing DataFrame with the new MeterValues DataFrame (that we just finished unpacking, exploding, and curating) and return a DataFrame that contains only the columns according to the below target schema:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- charge_duration_minutes: double (nullable = true)
# MAGIC  |-- charge_dispensed_Wh: double (nullable = true)
# MAGIC  ```

# COMMAND ----------

def join_transactions_with_meter_values(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    

df_transactions \
.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
) \
.transform(rename_timestamp_to_stop_timestamp) \
.transform(convert_start_stop_timestamp_to_timestamp_type) \
.transform(calculate_charge_duration_minutes) \
.transform(cleanup_extra_columns) \
.transform(
    join_transactions_with_meter_values, 
    df.filter(df.action == "MeterValues") \
    .transform(convert_metervalues_to_json) \
    .transform(flatten_metervalues_json) \
    .transform(get_most_recent_energy_active_import_register) \
    .transform(cast_value_to_double)
).show()

# COMMAND ----------

########## SOLUTION ###########
def join_transactions_with_meter_values(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df \
        .join(join_df, input_df.transaction_id == join_df.transaction_id, "left") \
        .select(input_df.transaction_id, input_df.charge_point_id, input_df.id_tag, input_df.start_timestamp, input_df.stop_timestamp, input_df.charge_duration_minutes, join_df.value.alias("charge_dispensed_Wh"))
    ###
    
df_transactions \
.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
) \
.transform(rename_timestamp_to_stop_timestamp) \
.transform(convert_start_stop_timestamp_to_timestamp_type) \
.transform(calculate_charge_duration_minutes) \
.transform(cleanup_extra_columns) \
.transform(
    join_transactions_with_meter_values, 
    df.filter(df.action == "MeterValues") \
    .transform(convert_metervalues_to_json) \
    .transform(flatten_metervalues_json) \
    .transform(get_most_recent_energy_active_import_register) \
    .transform(cast_value_to_double)
).show()

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_transactions_with_meter_values_unit

test_join_transactions_with_meter_values_unit(spark, join_transactions_with_meter_values)

# COMMAND ----------

from pyspark.sql.types import  TimestampType

def test_join_transactions_with_meter_values():
    result = df_transactions \
        .transform(
            join_transactions_with_stop_transactions,
            df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
        ) \
        .transform(rename_timestamp_to_stop_timestamp) \
        .transform(convert_start_stop_timestamp_to_timestamp_type) \
        .transform(calculate_charge_duration_minutes) \
        .transform(cleanup_extra_columns) \
        .transform(
            join_transactions_with_meter_values, 
            df.filter(df.action == "MeterValues") \
            .transform(convert_metervalues_to_json) \
            .transform(flatten_metervalues_json) \
            .transform(get_most_recent_energy_active_import_register) \
            .transform(cast_value_to_double)
        )
    result.show(5)
    result.printSchema()
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("transaction_id", IntegerType(), True), 
        StructField("charge_point_id", StringType(), True), 
        StructField("id_tag", StringType(), True), 
        StructField("start_timestamp", TimestampType(), True), 
        StructField("stop_timestamp", TimestampType(), True), 
        StructField("charge_duration_minutes", DoubleType(), True), 
        StructField("charge_dispensed_Wh", DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    print("All tests passed! :)")

test_join_transactions_with_meter_values()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC PHEW! That was a lot of work. Let's celebrate and have a look at our final DataFrame!

# COMMAND ----------

final_df = df_transactions \
.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
) \
.transform(rename_timestamp_to_stop_timestamp) \
.transform(convert_start_stop_timestamp_to_timestamp_type) \
.transform(calculate_charge_duration_minutes) \
.transform(cleanup_extra_columns) \
.transform(
    join_transactions_with_meter_values, 
    df.filter(df.action == "MeterValues") \
    .transform(convert_metervalues_to_json) \
    .transform(flatten_metervalues_json) \
    .transform(get_most_recent_energy_active_import_register) \
    .transform(cast_value_to_double)
)

final_df.show()
