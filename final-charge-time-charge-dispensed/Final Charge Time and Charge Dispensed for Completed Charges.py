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
# MAGIC \* The example payloads are decorated with a few extra fields including the `message_id`, `message_type`, and `charge_point_id` which generally comes from the CSMS. The actual payloads from the Charge Point are in the `body` field.
# MAGIC
# MAGIC In this exercise, we'll inspect the historical data that we have and create a CDR for all completed transactions.

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
# MAGIC Before we start to ingest our data, it's helpful to know in what direction we're going. A good guideline is to know where you want to be and work backwards to your data sources to identify how we'll need to transform or reshape our data. 
# MAGIC 
# MAGIC **Target Schema**
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC #### Reflect
# MAGIC Having looked at the example data in the table above, can you trace how to calculate these fields to the various OCPP events?

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
    
stop_transaction_request_df = df.transform(return_stop_transaction_requests)
display(stop_transaction_request_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test
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
# MAGIC #### E2E Test
# MAGIC And now the test to ensure that our real data is transformed the way we want.

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_return_stoptransaction_e2e

test_return_stoptransaction_e2e(df.transform(return_stop_transaction_requests), display_f=display)

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
# MAGIC **Your turn!**
# MAGIC Unpack the `body` column (currently a JSON string) for just the `StopTransaction` messages into a new column called `new_body` using the `with_column` and `from_json` functions and the following schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- meter_stop: integer (nullable = true)
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

display(df.transform(return_stop_transaction_requests).transform(convert_stop_transaction_request_json))



# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_stop_transaction_unit

test_convert_stop_transaction_unit(spark, convert_stop_transaction_request_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_stop_transaction_json_e2e

test_convert_stop_transaction_json_e2e(df.transform(return_stop_transaction_requests).transform(convert_stop_transaction_request_json), display_f=display)

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
    id_tag_info_schema = StructType([
        None
    ])

    schema = StructType([
        None
    ])
    ###

    return schema
    
    
def convert_start_transaction_response_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###


display(df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_response_json_unit

test_convert_start_transaction_response_json_unit(spark, convert_start_transaction_response_json)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_response_json_e2e
    
test_convert_start_transaction_response_json_e2e(
    df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json), display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_request_unit

test_convert_start_transaction_request_unit(spark, convert_start_transaction_request_json)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_transaction_request_json_e2e

    
test_convert_start_transaction_request_json_e2e(
    df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(
        convert_start_transaction_request_json),
    display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_with_start_transaction_request_unit

test_join_with_start_transaction_request_unit(spark, join_with_start_transaction_request)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_with_start_transaction_request_e2e
    
test_join_with_start_transaction_request_e2e(
    df.filter((df.action == "StartTransaction") & (df.message_type == 3)).transform(convert_start_transaction_response_json). \
    transform(join_with_start_transaction_request, df.filter((df.action == "StartTransaction") & (df.message_type == 2)).transform(convert_start_transaction_request_json)),
    display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_stop_with_start_unit

test_join_stop_with_start_unit(spark, join_stop_with_start)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_stop_with_start_e2e
    
test_join_stop_with_start_e2e(
    df.transform(return_stop_transaction_requests). \
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
                    ).transform(convert_start_transaction_request_json))),
    display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_stop_timestamp_to_timestamp_type_unit

test_convert_start_stop_timestamp_to_timestamp_type_unit(spark, convert_start_stop_timestamp_to_timestamp_type)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_start_stop_timestamp_to_timestamp_type_e2e

    
test_convert_start_stop_timestamp_to_timestamp_type_e2e(
    df.transform(return_stop_transaction_requests). \
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
    transform(convert_start_stop_timestamp_to_timestamp_type),
    display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_time_hours_unit

test_calculate_total_time_hours_unit(spark, calculate_total_time_hours)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_time_hours_e2e
    
test_calculate_total_time_hours_e2e(
    df.transform(return_stop_transaction_requests). \
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
        transform(calculate_total_time_hours),
        display_f=display
)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_energy_unit

test_calculate_total_energy_unit(spark, calculate_total_energy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_energy_e2e
    
test_calculate_total_energy_e2e(
    df.transform(return_stop_transaction_requests). \
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
        transform(calculate_total_energy),
    display_f=display
)

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
# MAGIC Very similarly to what we've done already for our `StartTransaction` Request and Response, we'll need to unpack this JSON string into JSON using the `from_json` function.
# MAGIC 
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

df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).printSchema()
df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_metervalues_to_json_unit

test_convert_metervalues_to_json_unit(spark, convert_metervalues_to_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_metervalues_to_json_e2e
    
test_convert_metervalues_to_json_e2e(
    df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json),
    display_f=display
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_parking_time
# MAGIC In our target object, there is a field `total_parking_time` which is the number of hours that the EV is plugged in but not charging. This denoted in the **Meter Values Request** by the `measurand` = `Power.Active.Import` where `phase` is `None` or `null` and a value of `0`.
# MAGIC 
# MAGIC While it might seem easy on the surface, the logic is actually quite complex and will require you to spend some time understanding [Windows](https://sparkbyexamples.com/pyspark/pyspark-window-functions/) in order for you to complete it. Don't worry, take your time to think through the problem!
# MAGIC 
# MAGIC We'll need to do this in a handful of steps:
# MAGIC 1. Build a DataFrame from our MeterValue Request data with `transaction_id`, `timestamp`, `measurand`, `phase`, and `value` pulled out as columns (explode)
# MAGIC 2. Return only rows with `measurand` = `Power.Active.Import` and `phase` = `Null`
# MAGIC 3. Figure out how to represent in the DataFrame when a Charger is actively charging or not charging, calculate the duration of each of those groups, and sum the duration of the non charging groups as the `total_parking_time`
# MAGIC 
# MAGIC **Notes**
# MAGIC * There may be many solutions but the focus should be on using the Spark built-in API
# MAGIC * You should be able to accomplish this entirely in DataFrames without for-expressions
# MAGIC 
# MAGIC **Target Schema**
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
# MAGIC  |-- total_parking_time: double(nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC **Hints**
# MAGIC * For deeply nested JSON, consider [exploding](https://sparkbyexamples.com/pyspark/pyspark-explode-array-and-map-columns-to-rows/) them out in order to access the values

# COMMAND ----------

from pyspark.sql.functions import explode, to_timestamp

def reshape_meter_values(input_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
    return input_df
    ###

result = df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values)
display(result)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_reshape_meter_values_unit

test_reshape_meter_values_unit(spark, reshape_meter_values)


# COMMAND ----------

# MAGIC %md
# MAGIC Now let's calculate the total parking time!

# COMMAND ----------

from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window


def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    
    ### YOUR CODE HERE
    window_by_transaction = Window.partitionBy("transaction_id").orderBy(col("timestamp").asc())
    return input_df
    ###

result = df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time)
display(result)







# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_parking_time_unit

test_calculate_total_parking_time_unit(spark, calculate_total_parking_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_total_parking_time_e2e

test_calculate_total_parking_time_e2e(
    df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time),
    display_f=display
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join with Target DataFrame
# MAGIC Now that we have the `total_parking_time`, we can join that with our Target Dataframe (where we stored our Stop/Start Transaction data).
# MAGIC 
# MAGIC Recall that our newly transformed DataFrame has the following schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC And our target dataframe:
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
# MAGIC  |-- total_parking_time: double(nullable = true)
# MAGIC  ```

# COMMAND ----------

def join_with_target_df(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
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
transform(calculate_total_energy).transform(join_with_target_df, df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time))
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_with_target_df_unit

test_join_with_target_df_unit(spark, join_with_target_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_with_target_df_e2e


test_join_with_target_df_e2e(
    df.transform(return_stop_transaction_requests). \
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
    transform(calculate_total_energy).transform(join_with_target_df, df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time)),
    display_f=display
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: All together now!
# MAGIC We took a few turns and side-quests but our join brought together all of the fields that we originally sought.
# MAGIC 
# MAGIC Our current schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: double (nullable = true)
# MAGIC  |-- meter_stop: double (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC Let's clean up our DataFrame so that we're left with the following schema:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC  ```

# COMMAND ----------

def cleanup_columns(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_cleanup_columns_unit

test_cleanup_columns_unit(spark, cleanup_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_cleanup_columns_e2e

test_cleanup_columns_e2e(
    df.transform(return_stop_transaction_requests). \
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
    transform(calculate_total_energy). \
    transform(join_with_target_df, df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time)).\
    transform(cleanup_columns),
    display_f=display
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC PHEW! That was a lot of work. Let's celebrate and have a look at our final DataFrame!

# COMMAND ----------

final_df = df.transform(return_stop_transaction_requests). \
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
    transform(calculate_total_energy). \
    transform(join_with_target_df, df.filter((df.action == "MeterValues") & (df.message_type == 2)).transform(convert_metervalues_to_json).transform(reshape_meter_values).transform(calculate_total_parking_time)).\
    transform(cleanup_columns)

display(result)


# COMMAND ----------


