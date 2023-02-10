# Databricks notebook source
# MAGIC %md
# MAGIC # Final Charge Time and Charge Dispensed for Completed Charges
# MAGIC After the Charge Point has registered itself with the CSMS (Charging Station Management System), it is able to send information via the OCPP protocol about Charging Sessions:
# MAGIC 
# MAGIC | OCPP Action | Description | Payload |
# MAGIC | --- | --- | --- | 
# MAGIC | StartTransaction | Message sent for Charging Sessions that have been initiated by the car (or by itself on a scheduled basis). This does not contain a transaction ID. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/starttransaction.json). 
# MAGIC | MeterValues | Message sent at a set frequency that samples energy throughput at various outlets. Measurand = "Energy.Active.Import.Register" gives a cumulative reading of the charge that has been dispensed. This contains a transaction ID. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/metervalues.json) |
# MAGIC | StopTransaction | Message sent about Charging Sessions that have been stopped by the car (or by itself based on set thresholds). It contains a transaction ID. | [example json](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/stoptransaction.json) |
# MAGIC 
# MAGIC One common question that are asked by EV owners and CPO (Charge Point Operators) alike is: **How much total charge has been dispensed for every completed transaction?**
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
# MAGIC Before we start to ingest our data, it's helpful to know in what direction we're going. In order to answer the question of **How much total charge has been dispensed for every completed transaction?** we'll need to know a few pieces of information (in green, below):
# MAGIC * a Transaction ID to identify the transaction (obtained from a transaction record [we have yet to ingest this), MeterValues, and StopTransaction)
# MAGIC * the Start Time (obtained from a transaction record) and End Time of the transaction (obtained from a StopTransaction)
# MAGIC * the Duration of the transaction in seconds which can be calculated by the Start and End Times
# MAGIC * the Charge Dispensed which can be taken from the last MeterValue reading for each Transaction (remember, there are many)
# MAGIC 
# MAGIC Our two data sources (in blue, below) required are the OCPP data (which we ingested in a previous exercise) and transaction records.
# MAGIC 
# MAGIC 💡 There's a lot of arrows. Take a deep breath and take it slowly!
# MAGIC 
# MAGIC ![entity-relationships-final-charge-duration-dispense.png](https://github.com/data-derp/exercise-ev-databricks/raw/main/assets/entity-relationships-final-charge-duration-dispense.png)

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

url = "https://github.com/data-derp/exercise-ev-databricks/blob/main/data_generator/out/data.csv?raw=true"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def create_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        StructField("charge_point_id", StringType(), True),
        StructField("write_timestamp", StringType(), True),
        StructField("action", StringType(), True),
        StructField("body", StringType(), True),
    ])
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("escape", "\"") \
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
# MAGIC ### EXERCISE: Read Transactions
# MAGIC Read in `transactions.csv` using the schema:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC ```

# COMMAND ----------

transactions_url = "https://raw.githubusercontent.com/data-derp/exercise-ev-databricks/main/data_generator/out/transactions.csv"
transactions_filepath = helpers.download_to_local_dir(transactions_url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_transactions_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        ### YOUR CODE HERE
        
        ###
    ])
    
    ### YOUR CODE HERE
    df = None
    ###
    return df
    
df_transactions = create_transactions_dataframe(transactions_filepath)
display(df_transactions)

# COMMAND ----------

def test_create_transactions_dataframe():
    result = create_transactions_dataframe(transactions_filepath)
    assert result is not None
    expected_columns = ['charge_point_id', 'id_tag', 'start_timestamp', 'transaction_id']
    assert result.columns == expected_columns, f"expected {expected_columns}, but got {result.columns}"
    count = result.count()
    expected_count = 27
    assert count == expected_count, f"expected {expected_count}, but got {count}"
    print("All tests pass! :)")
    
test_create_transactions_dataframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Return only StopTransaction
# MAGIC Recall that one of the first things we need to do is to take the transformations table and join it with the data for StopTransaction on `transaction_id`. Before we do that, we need a dataframe that only has StopTransaction data. Use the [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) method to return only data where the `action == "StopTransaction"`.

# COMMAND ----------

def return_stoptransaction(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.transform(return_stoptransaction).show()

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
            "action": "bar",
        },
        {
            "foo": "4496309f-dfc5-403d-a1c1-54d21b9093c1",
            "action": "StopTransaction",
        },
        {
            "foo": "bb7b2cd0-f140-4ffe-8280-dc462784303d",
            "action": "zebra",
        }

    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("foo", StringType()),
            StructField("action", StringType()),
        ])
    )

    result = input_df.transform(return_stoptransaction)
    result_count = result.count()
    assert result_count == 1, f"expected 1, but got {result_count}"

    result_actions = [x.action for x in result.collect()]
    expected_actions = ["StopTransaction"]
    assert result_actions == expected_actions, f"expect {expected_actions}, but got {result_actions}"

    print("All tests pass! :)")
    
test_return_stop_transaction_unit()

# COMMAND ----------

# MAGIC %md
# MAGIC And now the test to ensure that our real data is transformed the way we want.

# COMMAND ----------

def test_return_stoptransaction():
    result = df.transform(return_stoptransaction)
    
    count =  result.count()
    expected_count = 27
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

stoptransaction_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC That body contains the majority of the important information, including the `transaction_id`, upon which we need to join. Unfortunately, we can't query this string for `transaction_id`:

# COMMAND ----------

from pyspark.sql.functions import col

stoptransaction_df.select(col("body.transaction_id"))

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

from pyspark.sql.functions import from_json, json_tuple, col
from pyspark.sql.types import StringType, IntegerType, ArrayType, DoubleType, LongType


def stop_transaction_body_schema():
    return StructType([
        ### YOUR CODE HERE
        ###
    ])
    
def convert_stop_transaction_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

df.transform(return_stoptransaction).transform(convert_stop_transaction_json).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_convert_stop_transaction_unit

test_convert_stop_transaction_unit(spark, convert_stop_transaction_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the E2E test!

# COMMAND ----------

def test_convert_stop_transaction_json():
    result = df.transform(return_stoptransaction).transform(convert_stop_transaction_json)
    
    print("Transformed DF:")
    result.show()
    
    assert result.columns == ["charge_point_id", "write_timestamp", "action", "body", "new_body"]
    assert result.count() == 27, f"expected 27, but got {result.count()}"
    
    result_sub = result.limit(3)
    
    meter_stop = [x.meter_stop for x in result_sub.select(col("new_body.meter_stop")).collect()]
    expected_meter_stop = [26795, 32539, 37402]
    assert meter_stop == expected_meter_stop, f"expected {expected_meter_stop}, but got {meter_stop}"
    
    timestamps = [x.timestamp for x in result_sub.select(col("new_body.timestamp")).collect()]
    expected_timestamps = ["2022-10-02T15:56:17.000345+00:00", "2022-10-02T16:30:17.000345+00:00", "2022-10-03T00:56:23.000337+00:00"]
    assert timestamps == expected_timestamps, f"expected {expected_timestamps}, but got {timestamps}"
    
    transaction_ids = [x.transaction_id for x in result_sub.select(col("new_body.transaction_id")).collect()]
    expected_transaction_ids = [1, 2, 3]
    assert transaction_ids == expected_transaction_ids, f"expected {expected_transaction_ids}, but got {transaction_ids}"
    
    reasons = [x.reason for x in result_sub.select(col("new_body.reason")).collect()]
    expected_reasons = [None, None, None]
    assert reasons == expected_reasons, f"expected {expected_reasons}, but got {reasons}"
    
    id_tags = [x.id_tag for x in result_sub.select(col("new_body.id_tag")).collect()]
    expected_id_tags = ["7755461679280237103", "7755461679280237103", "15029309631809970278"]
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

stoptransaction_json_df = df.transform(return_stoptransaction).transform(convert_stop_transaction_json)
stoptransaction_json_df.select(col("new_body.transaction_id")).show(5)

# COMMAND ----------

stoptransaction_json_df.new_body.transaction_id

# COMMAND ----------

# MAGIC %md
# MAGIC Yes we can! :)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Flattening your Data
# MAGIC While we now can query the `new_body` column in its JSON form, we want to be a bit more forward thinking and instead flatten our data such that the embedded elements in the JSON are bubbled up to be columns themselves (and it additionally means that users of your data in the future won't need to convert from JSON-string to JSON or inquire as to the schema). In the field, this is known as curation and we'll talk about that in a later section.
# MAGIC 
# MAGIC In this exercise, we'll take all of the keys of the JSON embedded in `new_body` to be their own columns and only return those new columns and other relevant columns (below).
# MAGIC 
# MAGIC For reference, the current schema of `stoptransaction`:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- meter_stop: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- transaction_id: double (nullable = true)
# MAGIC  |    |-- reason: string (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- transaction_data: array (nullable = true)
# MAGIC  |    |    |-- element: string (containsNull = true)
# MAGIC  ```
# MAGIC  
# MAGIC  And the target schema:
# MAGIC  ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: double (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  ```

# COMMAND ----------

def flatten_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_flatten_json_unit

test_flatten_json_unit(spark, flatten_json)


# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

def test_flatten_data():
    result = df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    result.show()
    
    result_count = result.count()
    expected_count = 27
    
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_columns = result.columns
    expected_columns = ["charge_point_id", "write_timestamp", "action", "meter_stop", "timestamp", "transaction_id", "reason", "id_tag", "transaction_data"]
    assert result_columns == expected_columns, f"expected {expected_columns} but got {result_columns}"
        
    print("All tests pass! :)")
    
test_flatten_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Transactions with StopTransaction records that exist for those transactions
# MAGIC Back to the task at hand... we need to INNER join the transaction records with the StopTransaction records that exist for those `transaction_ids`. Use the [join] function to perform an inner join between `df_transactions` and `stoptransaction_json_df`.
# MAGIC 
# MAGIC Additionally, return only the following columns:
# MAGIC * charge_point_id (from the transactions data)
# MAGIC * id_tag (from the transactions data)
# MAGIC * start_timestamp (from the transactions data)
# MAGIC * transaction_id (from the transactions data)
# MAGIC * new_body (from the stop transactions data)
# MAGIC 
# MAGIC Expected schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)    // [return from transactions data]
# MAGIC  |-- charge_point_id: string (nullable = true)    // [return from transactions data]
# MAGIC  |-- id_tag: string (nullable = true)             // [return from transactions data]
# MAGIC  |-- start_timestamp: string (nullable = true)    // [return from joined data]
# MAGIC  |-- meter_stop: integer (nullable = true)        // [return from joined data]
# MAGIC  |-- timestamp: string (nullable = true)          // [return from joined data]
# MAGIC  |-- reason: string (nullable = true)             // [return from joined data]
# MAGIC  |-- transaction_data: array (nullable = true)    // [return from joined data]
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  ```
# MAGIC 
# MAGIC #### Reflect
# MAGIC * Why an inner join? What is excluded as part of an inner join?
# MAGIC * How do you identify `charge_point_id` (or any other ambiguous column) coming from the transactions dataset vs the stop transactions data?

# COMMAND ----------

def join_transactions_with_stop_transactions(input_df: DataFrame, join_df: DataFrame) -> DataFrame:    
    ### YOUR CODE HERE
    return input_df
    ###
    
df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_join_transactions_with_stop_transactions_unit

test_join_transactions_with_stop_transactions_unit(spark, join_transactions_with_stop_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

def test_join_transactions_with_stop_transactions():
    result = df_transactions.transform(
        join_transactions_with_stop_transactions,
        df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    )
    
    result.show()
    result.printSchema()
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_columns = result.columns
    expected_columns = ["transaction_id", "charge_point_id", "id_tag", "start_timestamp", "meter_stop", "timestamp", "reason", "transaction_data"]
    assert result_columns == expected_columns, f"expected {expected_columns}, but got {result_columns}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('transaction_id', IntegerType(), True), 
        StructField('charge_point_id', StringType(), True), 
        StructField('id_tag', StringType(), True), 
        StructField('start_timestamp', StringType(), True), 
        StructField('meter_stop', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('reason', StringType(), True), 
        StructField('transaction_data', ArrayType(StringType(), True), True)]
    )
    
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    
    print("All tests pass! :)")
    
test_join_transactions_with_stop_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Rename timestamp column to "stop_timestamp"
# MAGIC Note that after the join, we now have a column named `timestamp`, which in reality is our`stop_timestamp`. It might make sense to unambiguate that for someone who might encounter this data (and also for ourselves). 
# MAGIC 
# MAGIC Our current Dataframe after the join:
# MAGIC ```
# MAGIC +--------------+---------------+--------------------+--------------------+----------+--------------------+------+----------------+
# MAGIC |transaction_id|charge_point_id|              id_tag|     start_timestamp|meter_stop|           timestamp|reason|transaction_data|
# MAGIC +--------------+---------------+--------------------+--------------------+----------+--------------------+------+----------------+
# MAGIC |             1|         AL1000| 7755461679280237103|2022-10-02T15:28:...|     26795|2022-10-02T15:56:...|  null|            null|
# MAGIC |             2|         AL1000| 7755461679280237103|2022-10-02T15:58:...|     32539|2022-10-02T16:30:...|  null|            null|
# MAGIC |             3|         AL1000|15029309631809970278|2022-10-03T00:10:...|     37402|2022-10-03T00:56:...|  null|            null|
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC In this exercise, we'll use the [withColumnRenamed](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html) function to change the name of the `timestamp` column to `stop_timestamp`.
# MAGIC 
# MAGIC Our current Dataframe schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```
# MAGIC 
# MAGIC And the target schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

def rename_timestamp_to_stop_timestamp(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).transform(rename_timestamp_to_stop_timestamp).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_rename_timestamp_to_stop_timestamp_unit

test_rename_timestamp_to_stop_timestamp_unit(spark, rename_timestamp_to_stop_timestamp)

# COMMAND ----------

# MAGIC %md 
# MAGIC And now the E2E test!

# COMMAND ----------

def test_rename_timestamp_to_stop_timestamp():
    result = df_transactions.transform(
        join_transactions_with_stop_transactions,
        df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    ).transform(rename_timestamp_to_stop_timestamp)
    print("Transformed DF:")
    result.show()
    result.printSchema()
    
    result_columns = result.columns
    expected_columns = ["transaction_id", "charge_point_id", "id_tag", "start_timestamp", "meter_stop", "stop_timestamp", "reason", "transaction_data"]
    assert result_columns == expected_columns, f"expected {expected_columns}, but got {result_columns}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("transaction_id", IntegerType(), True), 
        StructField("charge_point_id", StringType(), True), 
        StructField("id_tag", StringType(), True), 
        StructField("start_timestamp", StringType(), True), 
        StructField("meter_stop", IntegerType(), True), 
        StructField("stop_timestamp", StringType(), True), 
        StructField("reason", StringType(), True), 
        StructField("transaction_data", ArrayType(StringType(), True), True)
    ])
    
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    print("All tests pass! :)")
    
test_rename_timestamp_to_stop_timestamp()
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Convert the start_timestamp and stop_timestamp fields to timestamp type
# MAGIC At some point soon, we'll need to calculate the time in minutes between the start and stop timestamp columns. However, note that both columns are of type `string`
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  ```
# MAGIC  
# MAGIC  In this exercise, we'll convert the `start_timestamp` and `stop_timestamp` columns to a timestamp type.
# MAGIC  
# MAGIC  Target schema:
# MAGIC  ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def convert_start_stop_timestamp_to_timestamp_type(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("start_timestamp", to_timestamp("start_timestamp")).withColumn("stop_timestamp", to_timestamp("stop_timestamp"))
    ###

df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).printSchema()

# COMMAND ----------

############ SOLUION #############
from pyspark.sql.functions import to_timestamp

def convert_start_stop_timestamp_to_timestamp_type(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.withColumn("start_timestamp", to_timestamp("start_timestamp")).withColumn("stop_timestamp", to_timestamp("stop_timestamp"))
    ###

df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).printSchema()

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
    result = df_transactions.transform(
        join_transactions_with_stop_transactions,
        df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    ).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type)
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField("transaction_id", IntegerType(), True), 
        StructField("charge_point_id", StringType(), True), 
        StructField("id_tag", StringType(), True), 
        StructField("start_timestamp", TimestampType(), True), 
        StructField("meter_stop", IntegerType(), True), 
        StructField("stop_timestamp", TimestampType(), True), 
        StructField("reason", StringType(), True), 
        StructField("transaction_data", ArrayType(StringType(), True), True),
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    print("All tests passed! :)")
    
test_convert_start_stop_timestamp_to_timestamp_type()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate the Charge Session Duration
# MAGIC Now that we have our `start_timestamp` and `stop_timestamp` columns in the appropriate type, we now can calculate the time in minutes of the charge.
# MAGIC 
# MAGIC Current schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```
# MAGIC 
# MAGIC Expected schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- charge_duration_minutes: double (nullable = true)       // NEW
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```
# MAGIC 
# MAGIC Round to two decimal places (note: it might not appear as two decimal places in the resulting Dataframe as a result of rendering).
# MAGIC 
# MAGIC **Hint**: You can convert a timestamp type to seconds using the [cast](...) function and the `long` type. Of course, that's just seconds. :bulb:

# COMMAND ----------

from pyspark.sql.functions import round

def calculate_charge_duration_minutes(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).transform(calculate_charge_duration_minutes).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_calculate_charge_duration_minutes_unit

test_calculate_charge_duration_minutes_unit(spark, calculate_charge_duration_minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E!

# COMMAND ----------

from pyspark.sql.types import TimestampType

def test_calculate_charge_duration_minutes():
    result = df_transactions.transform(
        join_transactions_with_stop_transactions,
        df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    ).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).transform(calculate_charge_duration_minutes)
    print("Transformed DF:")
    result.show()
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
        StructField("meter_stop", IntegerType(), True), 
        StructField("stop_timestamp", TimestampType(), True), 
        StructField("reason", StringType(), True), 
        StructField("transaction_data", ArrayType(StringType(), True), True),
        StructField("charge_duration_minutes", DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    result_charge_duration_minutes = [x.charge_duration_minutes for x in result.collect()]
    expected_charge_duration_minutes = [28.0, 32.0, 46.0, 16.0, 42.0, 36.0, 26.0, 46.0, 32.0, 8.0, 34.0, 42.0, 48.0, 28.0, 52.0, 46.0, 46.0, 10.0, 14.0, 50.0, 48.0, 26.0, 44.0, 12.0, 12.0, 14.0, 24.0]
    assert result_charge_duration_minutes == expected_charge_duration_minutes, f"expected {expected_charge_duration_minutes}, but got {result_charge_duration_minutes}"
    print("All tests passed! :)")
    
test_calculate_charge_duration_minutes()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Cleanup Extra Columns
# MAGIC 
# MAGIC Do we actually need all of these columns?
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC  |-- charge_duration_minutes: double (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC In this exercise, we'll remove some of the unneeded columns.
# MAGIC 
# MAGIC Target schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- charge_duration_minutes: double (nullable = true)
# MAGIC ```

# COMMAND ----------

def cleanup_extra_columns(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df_transactions.transform(
    join_transactions_with_stop_transactions,
    df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).transform(calculate_charge_duration_minutes).transform(cleanup_extra_columns).show()

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_cleanup_extra_columns_unit

test_cleanup_extra_columns_unit(spark, cleanup_extra_columns)

# COMMAND ----------

def test_cleanup_extra_columns():
    result = df_transactions.transform(
        join_transactions_with_stop_transactions,
        df.transform(return_stoptransaction).transform(convert_stop_transaction_json).transform(flatten_json)
    ).transform(rename_timestamp_to_stop_timestamp).transform(convert_start_stop_timestamp_to_timestamp_type).transform(calculate_charge_duration_minutes).transform(cleanup_extra_columns)
    print("Transformed DF:")
    result.show()
    result.printSchema()
    
    result_count = result.count()
    expected_count = 27
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_columns = result.columns
    expected_columns = ["transaction_id", "charge_point_id", "id_tag", "start_timestamp", "stop_timestamp"]
    assert result_columns == expected_columns, f"expected {expected_columns}, but got {result_columns}"
    
    result_schema = result.schema()
    expected_schema = StructType([
        StructField("transaction_id", IntegerType(), True), 
        StructField("charge_point_id", StringType(), True), 
        StructField("id_tag", StringType(), True), 
        StructField("start_timestamp", TimestampType(), True), 
        StructField("stop_timestamp", TimestampType(), True), 
        StructField("charge_duration_minutes", DoubleType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    print("All tests passed! :)")
    

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

# MAGIC %md
# MAGIC ### EXERCISE: Flatten MeterValues JSON
# MAGIC 
# MAGIC Similar to what we did for the StopTransaction Dataframe (it was a while ago!), we'll need to flatten out our data to more easily access/query our data. The MeterValues schema is a bit more complicated, involving 3 levels of nesting and arrays. From one record of MeterValue, we will multiple records as a result of flattening - each having the same `charge_point_id`, `write timestamp`, `action`, etc. For an example `MeterValue` such as the [sample JSON](https://github.com/data-derp/exercise-ev-databricks/blob/main/sample-data/metervalues.json), we should have as many records as we do `sampled_value`, which in this case is 20.
# MAGIC 
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
# MAGIC  Target Flattened Schema
# MAGIC  ```
# MAGIC  root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- value: string (nullable = true)
# MAGIC  |-- context: string (nullable = true)
# MAGIC  |-- format: string (nullable = true)
# MAGIC  |-- phase: string (nullable = true)
# MAGIC  |-- measurand: string (nullable = true)
# MAGIC  |-- unit: string (nullable = true)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC In this exercise, you'll use the `explode` function for array types and unpack maps using `*`. We've done the unpacking using `*` in a previous exercise, but `explode` is new AND fun! Let's quickly look at how it works:

# COMMAND ----------

# Define a DataFrame

explode_example_data = [
    (1,["giraffe","horse"]),
    (2,["elephant","cat",]),
    (3,["dog","cat"]),
    (4,None),
    (5,["rabbit","cat"])
]

explode_example_df = spark.createDataFrame(data=explode_example_data, schema = ["student_id","favourite_animals"])
explode_example_df.printSchema()
explode_example_df.show()

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode!

explode_example_df_result = explode_example_df.select(explode_example_df.student_id,explode(explode_example_df.favourite_animals).alias("favourite_animal"))
explode_example_df_result.printSchema()
explode_example_df_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Now back to the exercise...

# COMMAND ----------

def flatten_metervalues_json(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_flatten_metervalues_json_unit

test_flatten_metervalues_json_unit(spark, flatten_metervalues_json)

# COMMAND ----------

# MAGIC %md
# MAGIC And now the E2E test!

# COMMAND ----------

def test_flatten_metervalues_json():
    result = df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json)

    print("Transformed DF:")
    result.show(5)
    result.printSchema()
    
    result_count = result.count()
    expected_count = 8080
    assert result_count == expected_count, f"expected {expected_count}, but got {result_count}"
    
    result_schema = result.schema
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('connector_id', IntegerType(), True), 
        StructField('transaction_id', IntegerType(), True), 
        StructField('timestamp', StringType(), True), 
        StructField('value', StringType(), True), 
        StructField('context', StringType(), True), 
        StructField('format', StringType(), True), 
        StructField('phase', StringType(), True), 
        StructField('measurand', StringType(), True), 
        StructField('unit', StringType(), True)
    ])
    assert result_schema == expected_schema, f"expected {expected_schema}, but got {result_schema}"
    
    result_measurand = set([ x.measurand for x in result.collect() ])
    expected_measurand = set(['Current.Import', 'Energy.Active.Import.Register', 'Voltage', 'Power.Active.Import'])
    assert result_measurand == expected_measurand, f"expected {expected_measurand}, but got {result_measurand}"
    print("All tests passed! :)")
    
test_flatten_metervalues_json()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Most recent Energy.Active.Import.Register Reading
# MAGIC In this exercise we'll be getting the most recent MeterValue reading for measurand `Energy.Active.Import.Register` for each `charge_point_id` and `transaction_id`.
# MAGIC 
# MAGIC Let's first check out the data before we start to transform it. Notice that there are quite a few `Energy.Active.Import.Register` per `charge_point_id` and `transaction_id`.
# MAGIC 
# MAGIC **Hint:** When sorting for the most recent, what column do you need to sort by? And what type does it need to be, in order to sort?
# MAGIC 
# MAGIC **Second hint:** `write_timestamp` represents the time at which the record was collected by the CSMS. As with most IoT devices, sometimes there is a network error which causes a build-up of events which are queued and then send en-masse once the network connection is back. Is there another column you can use?

# COMMAND ----------

df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).filter(col("measurand") == "Energy.Active.Import.Register").show()

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import row_number

def get_most_recent_energy_active_import_register(input_df: DataFrame) -> DataFrame:
    return input_df \
        .filter(col("measurand") == "Energy.Active.Import.Register") \
        
df.filter(df.action == "MeterValues").transform(convert_metervalues_to_json).transform(flatten_metervalues_json).transform(get_most_recent_energy_active_import_register).show(10)

# COMMAND ----------

pyspark.sql.types.TimestampTypefrom exercise_ev_databricks_unit_tests.final_charge_time_charge_dispensed_completed_charges import test_get_most_recent_energy_active_import_register_unit

test_get_most_recent_energy_active_import_register_unit(spark, get_most_recent_energy_active_import_register)

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
