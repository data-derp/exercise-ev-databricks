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
# MAGIC * Write to Parquet
# MAGIC
# MAGIC **NOTE:** You've already done these exercises before. We absolutely recommend bringing over your answers from that exercise to speed things along (with some minor tweaks), because you already know how to do all of that already! Of course, you're welcome to freshly rewrite your answers to test yourself!

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
# MAGIC Let's read the parquet files that we created in the Silver layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Silver layer but for simplicity and consistent results [of this exercise], we're going to read in a Silver output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!

# COMMAND ----------

meter_values_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/MeterValuesRequest/part-00000-tid-468425781006758111-f9d48bc3-3b4c-497e-8e9c-77cf63db98f8-207-1-c000.snappy.parquet"
start_transaction_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionRequest/part-00000-tid-9191649339140138460-0a4f58e5-1397-41cc-a6a1-f6756f3332b6-218-1-c000.snappy.parquet"
start_transaction_response_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionResponse/part-00000-tid-5633887168695670016-762a6dfa-619c-412d-b7b8-158ee41df1b2-185-1-c000.snappy.parquet"
stop_transaction_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StopTransactionRequest/part-00000-tid-5108689541678827436-b76f4703-dabf-439a-825d-5343aabc03b6-196-1-c000.snappy.parquet"

meter_values_request_filepath = helpers.download_to_local_dir(meter_values_request_url)
start_transaction_request_filepath = helpers.download_to_local_dir(start_transaction_request_url)
start_transaction_response_filepath = helpers.download_to_local_dir(start_transaction_response_url)
stop_transaction_request_filepath = helpers.download_to_local_dir(stop_transaction_request_url)


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
start_transaction_request_df = read_parquet(start_transaction_request_filepath)
start_transaction_response_df = read_parquet(start_transaction_response_filepath)
stop_transaction_request_df = read_parquet(stop_transaction_request_filepath)
meter_values_request_df = read_parquet(meter_values_request_filepath)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_match_start_transaction_requests_with_responses_unit

test_match_start_transaction_requests_with_responses_unit(spark, match_start_transaction_requests_with_responses)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_match_start_transaction_requests_with_responses_e2e

test_match_start_transaction_requests_with_responses_e2e(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df), display)



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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_join_with_start_transaction_responses_unit

test_join_with_start_transaction_responses_unit(spark, join_with_start_transaction_responses)


# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_join_with_start_transaction_responses_e2e

test_join_with_start_transaction_responses_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ), display)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_time_unit

test_calculate_total_time_unit(spark, calculate_total_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_time_hours_e2e

test_calculate_total_time_hours_e2e(
    stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time), display
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_energy
# MAGIC Calculate total_energy (withColumn, cast)
# MAGIC Using [withColumn](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn) and [cast](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast), calculate the total energy by subtracting `meter_stop` from `meter_start`, converting that value from Wh (Watt-hours) to kWh (kilo-Watt-hours), and rounding to the nearest 2 decimal points.
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
# MAGIC
# MAGIC  **Hint:** Wh -> kWh = divide by 1000

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_energy_unit

test_calculate_total_energy_unit(spark, calculate_total_energy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_energy_e2e

test_calculate_total_energy_e2e(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy), display)

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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_parking_time_unit

test_calculate_total_parking_time_unit(spark, calculate_total_parking_time)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_calculate_total_parking_time_e2e

test_calculate_total_parking_time_e2e(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time), display)


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

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_join_and_shape_unit

test_join_and_shape_unit(spark, join_and_shape)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_join_and_shape_df_e2e

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
     ), display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write to Parquet
# MAGIC In this exercise, write the DataFrame `f"{out_dir}/cdr"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------


def write_to_parquet(input_df: DataFrame):
    output_directory = f"{out_dir}/cdr"
    ### YOUR CODE HERE
    input_df
    ###

write_to_parquet(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     ))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/cdr")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_gold import test_write_to_parquet

test_write_to_parquet(spark, dbutils, out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congratulations on finishing the Gold Tier exercise! Compared to a previous exercise where we did some of these exact exercises, you might have noticed that this time around, it was significantly easier to comprehend and complete because we didn't need to perform as many repetitive transformations to get to the interesting business logic.
# MAGIC
# MAGIC * What might you do with this data now that you've transformed it?

# COMMAND ----------


