# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming
# MAGIC It is very common for a Charge Point Operator to want to understand current status of their fleet of chargers, which might contain information if the Charger needs repairing. In this exercise, our goal is to continuously report on the **Status of Chargers in 5-minute intervals**. To achieve this, we'll put our knowledge of Structured Streaming to the test by:
# MAGIC
# MAGIC * Ingesting a Data Stream (StatusNotification Requests)
# MAGIC * Unpack the JSON string so that we can extract the status
# MAGIC * Ignore late data past 10 minutes and aggregate data over 5-minute tumbling windows
# MAGIC * Write to storage
# MAGIC
# MAGIC Sample [OCPP](https://en.wikipedia.org/wiki/Open_Charge_Point_Protocol) StatusNotification Request:
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
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests freezegun ocpp

# COMMAND ----------

exercise_name = "structured_streaming"

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
# MAGIC In lieu of real streaming infrastructure (like Kafka), we'll use the Spark Streaming Rate source to stream data into our notebook. This Rate Stream has two fields, `timestamp` and `value` (0..n). Reading our data into a Pandas DataFrame allows us to leverage the DataFrame's index (0..n) to create a column to join the Rate Stream `value` field/column with the sample data's index column. 

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, lit

# In a real streaming workload, set this to the total number of CPU cores 
# provided by your worker nodes in your cluster to minimize excessive shuffling in streaming.
spark.conf.set("spark.sql.shuffle.partitions", 8)

url = "https://raw.githubusercontent.com/kelseymok/charge-point-live-status/main/ocpp_producer/data/1683036538.json"
pandas_df = pd.read_json(url, orient='records')
pandas_df["index"] = pandas_df.index

mock_data_df = spark.createDataFrame(pandas_df)

# COMMAND ----------

mock_data_df.printSchema()

# COMMAND ----------

display(mock_data_df.select("index"))

# COMMAND ----------

display(mock_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Read from Stream
# MAGIC Set the streaming format to the Rate source and set the rows per section to 10.

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
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit Test

# COMMAND ----------


from typing import Callable
import json
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, LongType

def test_read_from_stream(spark, f: Callable):
    #Test that the resulting schema is correct

    input_pandas = pd.DataFrame([
        {
            "value": 0,
            "charge_point_id": "430ca2a2-54a6-4247-9adf-d86300231c62",
            "action": "StatusNotification",
            "message_type": 2,
            "body": "123"
        }     
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType
        ([
            StructField("value", LongType(),True),
            StructField("charge_point_id", StringType(),True),
            StructField("action", StringType(),True),
            StructField("message_type", LongType(),True),
            StructField("body", StringType(),True)
        ])
    )

    print("-->Input Schema")
    input_df.printSchema()

    result = input_df.transform(f)
    print("Transformed DF")

    print("-->Result Schema")
    result.printSchema()
    
    #Test 1
    # Not Possible as result.count() does not work for streaming dataframes

    #Test 2 
    # Not Possible as result.collect() does not work for streaming dataframes

    #Test 3
    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField("value", LongType(),True),
            StructField("charge_point_id", StringType(),True),
            StructField("action", StringType(),True),
            StructField("message_type", LongType(),True),
            StructField("body", StringType(),True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"


    print("All tests pass! :)")

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

display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

import pandas as pd
from typing import Callable
import json
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

def test_unpack_json_from_status_notification_request_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "123",
            "body": json.dumps({
                    "connector_id": 1, 
                    "error_code": "NoError", 
                    "status": "Available", 
                    "timestamp": "2023-01-01T09:00:00+00:00", 
                    "info": None, 
                    "vendor_id": None, 
                    "vendor_error_code": None
                })
        },
        {
            "charge_point_id": "456",
            "body": json.dumps({
                "connector_id": 1, 
                "error_code": "NoError", 
                "status": "SuspendedEVSE", 
                "timestamp": "2023-01-01T09:03:00+00:00", 
                "info": None, 
                "vendor_id": None, 
                "vendor_error_code": None
            })
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
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

    result_data = [(x.charge_point_id, x.new_body.status) for x in result.collect()]
    expected_data = [("123", "Available"), ("456", "SuspendedEVSE")]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField("charge_point_id", StringType(),True),
            StructField("body", StringType(),True),
            StructField("new_body", StructType([
                StructField("connector_id", IntegerType(),True),
                StructField("error_code", StringType(),True),
                StructField("status", StringType(),True),
                StructField("timestamp", StringType(),True),
                StructField("info", IntegerType(),True),
                StructField("vendor_id", IntegerType(),True),
                StructField("vendor_error_code", IntegerType(),True)
            ]),True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"

    print("All tests pass! :)")


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

# To display data in the resulting dataframe
display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns))

# df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------



import pandas as pd
from typing import Callable
import json
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, TimestampType
import datetime


def test_select_columns_unit(spark, f: Callable):
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "430ca2a2-54a6-4247-9adf-d86300231c62",
            "new_body": json.dumps({"status": "SuspendedEV", "timestamp":  "2023-01-01T09:00:00.000+0000"})
        },
         {
            "charge_point_id": "f3cf5e5a-701e-410a-9739-b00cda3f082c",
            "new_body": json.dumps({"status": "Faulted", "timestamp":  "2023-01-01T09:00:00.000+0000"})
        }
    ])

    body_schema = StructType([
            StructField("status", StringType(),True),
            StructField("timestamp", StringType(),True)

        ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("new_body", StringType())
        ])
    ).withColumn("new_body", from_json("new_body", body_schema))

    result = input_df.transform(f)
    print("Transformed DF")
    result.show(truncate=False)

    #Test # 1
    result_count = result.count()
    expected_count = 2
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    #Test # 2
    result_data = [(x.charge_point_id, x.status, x.timestamp) for x in result.collect()]
    expected_data = [("430ca2a2-54a6-4247-9adf-d86300231c62", "SuspendedEV", datetime.datetime(2023, 1, 1, 9, 0)), ("f3cf5e5a-701e-410a-9739-b00cda3f082c", "Faulted", datetime.datetime(2023, 1, 1, 9, 0))]
    assert result_data == expected_data, f"Expected {expected_data}, but got {result_data}"

    #Test # 3
    result_schema = result.schema
    expected_schema = StructType(
        [
            StructField("charge_point_id", StringType(),True),
            StructField("status", StringType(),True),
            StructField("timestamp", TimestampType(),True)
        ])
    assert result_schema == expected_schema, f"Expected {expected_schema}, but got {result_schema}"
    
    print("All tests pass! :)")

test_select_columns_unit(spark, select_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Aggregate, set up window and watermark
# MAGIC In this exercise, we'll want to create a tumbling window of 5 minutes and 10-minute lateness threshold for controlling our watermarks. We also want to group by `charge_point_id` and `status`, then compute the following statistics in each 5-minute window:
# MAGIC - count of status updates, alias this as `count_status_updates`
# MAGIC - min timestamp detected, alias this as `min_timestamp_detected`
# MAGIC - max timestamp detected, alias this as `max_timestamp_detected`
# MAGIC
# MAGIC To understand watermarks better, see this [section](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
# MAGIC
# MAGIC *"You can define the watermark of a query by specifying the **event time column** and the **threshold on how late the data is expected to be** in terms of event time. For a specific window ending at time T, the engine will maintain state and allow late data to update the state until (max event time seen by the engine - late threshold > T). In other words, late data within the threshold will be aggregated, but data later than the threshold will start getting dropped (see later in the section for the exact guarantees)."*

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
        .agg(
            F.count(col("status")).alias("count_status_updates"),
            # [PAWARIT] - add a few columns to help sanity check 
            F.min(col("timestamp")).alias("min_timestamp_detected"), 
            F.max(col("timestamp")).alias("max_timestamp_detected"),
            )
    ###

# df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).transform(aggregate_window_watermark).printSchema()

display(df.transform(read_from_stream).transform(unpack_json_from_status_notification_request).transform(select_columns).transform(aggregate_window_watermark), outputMode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test
# MAGIC @Syed: not sure if this is possible but have a think about it, maybe do some research - don't spend too much time on this if it's too complicated. But we should say why we don't have a unit test if we don't do it here. I THINK we should be able to test for the schema shape which should be a good start. Is there a way to get some metadata about the watermarks and the window time?
# MAGIC
# MAGIC @kelsey: Watermarks I'm not sure, will do some little research. But windows, as you see, we get the start and end time. Any other metadata you are looking for ?
# MAGIC
# MAGIC [Pawarit]: Yes, definitely get the window as an extra column to help people understand + sanity check. The unit test below was not working correctly because of two reasons:
# MAGIC   1. If you use the batch DataFrame APIs, the watermarks become irrelevant/ignored
# MAGIC   2. To disregard late arriving data, you actually depend on *newly arriving* records to close past windows. For example, if you have data from 09:01 (which would belong to the 09:00-09:05 window) and your lateness threshold is 10 minutes, you'll need to wait to see a record from later than 09:15 in order to finalize and send off the 09:00-09:05 window.

# COMMAND ----------


from typing import Callable
import json
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, LongType

def test_aggregate_window_watermark_unit(spark, f: Callable):
    
    sequence = [
      {
          "charge_point_id": "444984d5-0b9c-474e-a972-71347941ae0e",
          "status": "Reserved",
          "timestamp": "2023-01-01T09:00:00.000+0000",
      },
      {
          "charge_point_id": "444984d5-0b9c-474e-a972-71347941ae0e",
          "status": "Reserved",
          "timestamp": "2023-01-01T09:16:00.000+0000", # this record shouldn't show up because there's no later record to close this window yet. We would need something later than 09:30 to close this record's window
      },
      {
          "charge_point_id": "444984d5-0b9c-474e-a972-71347941ae0e",
          "status": "Reserved",
          "timestamp": "2023-01-01T09:04:59.000+0000", # this record should be ignored due to lateness
      }
    ]
    input_schema = StructType([
            StructField("charge_point_id", StringType()),
            StructField("status", StringType()),
            StructField("timestamp", StringType())]
            )

    exercise_directory = f"{working_directory}/watermark_exercise/"
    dbutils.fs.rm(f"dbfs:{exercise_directory}", True)

    input_location = f"{exercise_directory}/input_stream/"
    output_location = f"{exercise_directory}/output_stream/"
    checkpoint_location = f"{exercise_directory}/checkpoint/"

    # To properly simulate the effect of watermarking,
    # we need to run across multiple triggers.
    # Spark will first check for the latest timestamp in the current trigger,
    # then use that as the criteria for dropping late events in the *next* trigger.
    # The easiest way to achieve this control is to use:
    # - Delta Lake as a streaming sink and source with trigger(availableNow=True)
    # - A simple Python for loop to move to the next trigger 
    for record in sequence:

      record_pandas = pd.DataFrame([record])
      record_df = (spark.createDataFrame(record_pandas, input_schema)
          .withColumn("timestamp", to_timestamp("timestamp"))
      )
      record_df.write.mode("append").format("delta").save(input_location)

      incoming_data = (
          spark
          .readStream.format("delta")
          .load(input_location)
          .transform(f)
      )

      streaming_query = (
          incoming_data
          .writeStream
          .format("delta")
          .option("path", output_location)
          .option("checkpointLocation", checkpoint_location)
          .option("outputMode", "append")
          .trigger(availableNow=True)
          .start()
          .awaitTermination()
      )

    result = spark.read.format("delta").load(output_location)
    print("-->Result Schema")
    result.printSchema()

    # Test schema internals:
    # This case is a little trickier because the timeWindow field
    # contains more metadata such as watermarkDelayMs 
    result_schema_json_string = result.schema.json()
    expected_schema_json_string = '{"fields":[{"metadata":{},"name":"charge_point_id","nullable":true,"type":"string"},{"metadata":{},"name":"status","nullable":true,"type":"string"},{"metadata":{"spark.timeWindow":true,"spark.watermarkDelayMs":600000},"name":"window","nullable":true,"type":{"fields":[{"metadata":{},"name":"start","nullable":true,"type":"timestamp"},{"metadata":{},"name":"end","nullable":true,"type":"timestamp"}],"type":"struct"}},{"metadata":{},"name":"count_status_updates","nullable":true,"type":"long"},{"metadata":{},"name":"min_timestamp_detected","nullable":true,"type":"timestamp"},{"metadata":{},"name":"max_timestamp_detected","nullable":true,"type":"timestamp"}],"type":"struct"}'

    assert result_schema_json_string == expected_schema_json_string, f"""Expected {result_schema_json_string}, but got {expected_schema_json_string}"""

    result_records = [x.asDict(True) for x in result.collect()]
    expected_records = [
      {
        'charge_point_id': '444984d5-0b9c-474e-a972-71347941ae0e', 
        'status': 'Reserved', 
        'window': {
          'start': datetime.datetime(2023, 1, 1, 9, 0), 
          'end': datetime.datetime(2023, 1, 1, 9, 5)
        }, 
        'count_status_updates': 1, 
        'min_timestamp_detected': datetime.datetime(2023, 1, 1, 9, 0), 
        'max_timestamp_detected': datetime.datetime(2023, 1, 1, 9, 0)
        }
    ]
    assert result_records == expected_records, f"Expected {expected_records}, but got {result_records}"

    result_count = result.count()
    expected_count = 1
    assert result_count == expected_count, f"Expected {expected_count}, but got {result_count}"

    dbutils.fs.rm(f"dbfs:{exercise_directory}", True)
    print("All tests pass! :)")

test_aggregate_window_watermark_unit(spark, aggregate_window_watermark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write to Remote Storage
# MAGIC In this exercise, we'll write to our output directory periodically in the csv format (just for readability)

# COMMAND ----------

#TODO
output_dir = f"{working_directory}/output/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## All together now!

# COMMAND ----------

display(df.\
    transform(read_from_stream).\
    transform(unpack_json_from_status_notification_request).\
    transform(select_columns).\
    transform(aggregate_window_watermark), outputMode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2E Test
# MAGIC Check that the relevant files exist

# COMMAND ----------

spark.createDataFrame(dbutils.fs.ls(output_dir))

# COMMAND ----------

query = agg_df \
    .writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("counts")\
    .option('truncate', 'false') \
    .start()


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   `Time Window`,
# MAGIC   `Charge Point Id`,
# MAGIC   `Status Count`
# MAGIC from
# MAGIC   counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   date_format(`Time Window`.end,
# MAGIC   "MMM-dd HH:mm"
# MAGIC ) as time
# MAGIC from
# MAGIC   counts

# COMMAND ----------

# MAGIC %md
# MAGIC We can now see our data updated in a table in real time!

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Clean-up
# MAGIC - Shut down all streams

# COMMAND ----------

import time

time.sleep(300)
for s in spark.streams.active:
  try:
      s.stop()
  except:
      pass
