# Databricks notebook source
# MAGIC %md
# MAGIC # Stateful Streaming
# MAGIC It is very common for a Charge Point Operator to want to understand current status of their fleet of chargers, which might contain information if the Charger needs repairing. In this exercise, we'll use our knowledge of Stateful streaming to report on the **Status of Chargers in the last 5 minutes**. In this exercise, we'll put our Stateful Streaming knowledge to the test by:
# MAGIC
# MAGIC * Ingesting a Data Stream (StatusNotification Requests)
# MAGIC * Unpacking the JSON string so that we can extract the `status` field
# MAGIC * Ignoring late data past 10 minutes and aggregate data over the previous 5 minutes
# MAGIC * Writing to storage
# MAGIC
# MAGIC Sample OCPP StatusNotification Request:
# MAGIC ```json
# MAGIC {
# MAGIC   "connector_id": 1, 
# MAGIC   "error_code": "NoError", 
# MAGIC   "status": "Available", 
# MAGIC   "timestamp": "2023-01-01T09:00:00+00:00", 
# MAGIC   "info": null, 
# MAGIC   "vendor_id": null, 
# MAGIC   "vendor_error_code": null
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "stateful_streaming"

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
# MAGIC ## Read StatusNotification Request Data
# MAGIC In lieu of real streaming infrastructure (like Kafka, which we won't set up for financial reasons), we'll use the Spark Streaming Rate source to stream data into our notebook. This Rate Stream has two fields, `timestamp` and `value` (0..n). Reading our data into a Pandas DataFrame allows us to leverage the DataFrame's index (0..n) to create a column to join the Rate Stream `value` field/column with the sample data's index column. 

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col,lit

url = "https://raw.githubusercontent.com/kelseymok/charge-point-live-status/main/ocpp_producer/data/1683036538.json"
pandas_df = pd.read_json(url, orient='records')
pandas_df["index"] = pandas_df.index
mock_data_df = spark.createDataFrame(pandas_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Read from Stream
# MAGIC Set the streaming format to the Rate source and set the rows per section to 10.

# COMMAND ----------


from pyspark.sql import DataFrame

def read_from_stream(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    raw_stream_data = (
        spark.readStream.format(None)
        .option("rowsPerSecond", None)
        .load()
    )
    ###


    # This is just data setup, not part of the exercise
    return raw_stream_data.\
        join(mock_data_df, raw_stream_data.value == mock_data_df.index, 'left').\
        drop("timestamp").\
        drop("index")


df = read_from_stream(mock_data_df)
# display(df)


# COMMAND ----------


############ SOLUTION ##############
from pyspark.sql import DataFrame

def read_from_stream(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    raw_stream_data = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 10)
        .load()
    )
    ###


    # This is just data setup, not part of the exercise
    return raw_stream_data.\
        join(mock_data_df, raw_stream_data.value == mock_data_df.index, 'left').\
        drop("timestamp").\
        drop("index")


df = read_from_stream(mock_data_df)
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.stateful_streaming import test_read_from_stream

test_read_from_stream(spark, read_from_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Unpack JSON from Status Notification Request
# MAGIC Similar to our previous exercises, in order to read a json string, we need to use [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn) and [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html) to be able to extract values from it. Extract the result into a new column called `new_body`.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- error_code: string (nullable = true)
# MAGIC  |    |-- status: string (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- info: integer (nullable = true)
# MAGIC  |    |-- vendor_id: integer (nullable = true)
# MAGIC  |    |-- vendor_error_code: integer (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, StructField, StructType


def unpack_json_from_status_notification_request(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    body_schema = None

    return input_df
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request))

# COMMAND ----------

############## SOLUTION #################

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

def unpack_json_from_status_notification_request(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("info", IntegerType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("vendor_error_code", IntegerType(), True),
    ])

    return input_df.withColumn("new_body", from_json(col("body"), schema=body_schema))
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.stateful_streaming import test_unpack_json_from_status_notification_request_unit

test_unpack_json_from_status_notification_request_unit(spark, unpack_json_from_status_notification_request)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Select Columns
# MAGIC In this exercise, we'll pull out the `status` and `timestamp` fields from the `new_body` column that was created in the previous exercise. We'll also do a little bit of clean up by either dropping the remaining fields or using `select` to choose only the `charge_point_id`, `status`, and `timestamp` columns.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- status: string (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, expr, to_timestamp
import pyspark.sql.functions as F

def select_columns(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns))

# COMMAND ----------

########### SOLUTION ###########
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, expr, to_timestamp
import pyspark.sql.functions as F

def select_columns(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df \
        .withColumn("status", expr("new_body.status"))\
        .withColumn("timestamp", to_timestamp(col("new_body.timestamp")))\
        .select("charge_point_id", "status", "timestamp")
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.stateful_streaming import test_select_columns_unit

test_select_columns_unit(spark, select_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Aggregate, set up window and watermark
# MAGIC In this exercise, we'll want to create a tumbling window of 5 minutes using a [groupBy](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html) and [window](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.window.html) and expire events that are older than 10 minutes by using [withWatermark](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withWatermark.html). We also want to [groupBy](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html) `charge_point_id` and `status`, and [sum](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.GroupedData.sum.html) the number of status updates in the window period. 

# COMMAND ----------

from pyspark.sql.functions import from_json, window

def aggregate_window_watermark(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).transform(aggregate_window_watermark), outputMode="update")

# COMMAND ----------

############# SOLUTION ##############
from pyspark.sql.functions import from_json, window

def aggregate_window_watermark(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df\
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(col("charge_point_id"),
                col("status"),
                window(col("timestamp"), "5 minutes"))\
        .agg(F.count(col("status")))
    ###

# display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).transform(aggregate_window_watermark), outputMode="update")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.stateful_streaming import test_aggregate_window_watermark_unit

test_aggregate_window_watermark_unit(spark, aggregate_window_watermark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Visualise the Stream
# MAGIC In this exercise, we'll visualise our stream by creating a bar graph that shows changing counts on the y-axis and the charge points on the x-axis. Click the **+** sign in the table and select the relevant columns for the axes.

# COMMAND ----------

display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).transform(aggregate_window_watermark), outputMode="update")

# COMMAND ----------

# MAGIC %md
# MAGIC We can now see our data updated in a table in real time!

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write to Remote Storage
# MAGIC In this exercise, we'll write to our output directory periodically in the parquet format so that others might be able to consume our data (albeit in a different format).

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's set up some important directories.
# MAGIC * Output directory
# MAGIC * Checkpoint directory

# COMMAND ----------

# Output directory
out_dir = f"{working_directory}/output/"
print(f"Output directory: {out_dir}")

# Checkpoint directory
checkpoint_dir = f"{working_directory}/checkpoint/"
print(f"Checkpoint directory: {checkpoint_dir}")

# COMMAND ----------

def write_aggregate_window_watermark(input_df: DataFrame):
    ### YOUR CODE HERE
    input_df\
        .writeStream\
            .option("checkpointLocation", None)\
            .option("path", None)\
            .format("parquet")\
            .start()
    ###

write_aggregate_window_watermark(df.\
    transform(read_from_stream).\
    transform(unpack_json_from_status_notification_request).\
    transform(select_columns).\
    transform(aggregate_window_watermark))



# COMMAND ----------

############### SOLUTION #################

def write_aggregate_window_watermark(input_df: DataFrame):
    ### YOUR CODE HERE
    input_df\
        .writeStream\
            .option("checkpointLocation", checkpoint_dir)\
            .option("path", out_dir)\
            .format("parquet")\
            .start()
    ###

write_aggregate_window_watermark(df.\
    transform(read_from_stream).\
    transform(unpack_json_from_status_notification_request).\
    transform(select_columns).\
    transform(aggregate_window_watermark))

# COMMAND ----------

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2E Test
# MAGIC Check that the relevant files exist

# COMMAND ----------

from exercise_ev_databricks_unit_tests.stateful_streaming import test_write_e2e

test_write_e2e(spark, dbutils, out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up
# MAGIC Now that we're done with the exercise, let's clean up all the streams (otherwise they run forever and incur costs) and remove the output/checkpoint directories.

# COMMAND ----------

helpers.stop_all_streams(spark)

dbutils.fs.rm(out_dir, True)
dbutils.fs.rm(checkpoint_dir, True)
