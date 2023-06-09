# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake and our Domain (Silver)
# MAGIC In the Silver layer, we'll clean-up and harmonise our data without massive reducing the dimensions of data:
# MAGIC 1. Ingest data from the Bronze layer from the last minute
# MAGIC 2. Transform (flatten, standardise, harmonise, deduplicate)
# MAGIC 3. Write the Event Log in the Delta Lake format to a dedicated storage location
# MAGIC 4. Write several Delta Lake tables for each of the OCPP actions that we handle to a dedicated storage location
# MAGIC
# MAGIC **Note:** for this notebook to work as expected, make sure that the Bronze notebook is running/streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers databricks_helpers exercise_ev_production_code_delta_lake exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise-ev-production-code-delta-lake#egg=exercise_ev_production_code_delta_lake git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "delta_lake_domain_silver"

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
# MAGIC ## EXERCISE: Read Partition
# MAGIC When we [eventually] read our data from the Bronze Delta Lake output directory, we shouldn't need to specify a read path (e.g. `load("my-awesome-dir/action=StartTransaction")`) but rather, the `where` function should be able to figure out where the partitioned files live, and not query the entire dataset, just the partition.
# MAGIC
# MAGIC ```python
# MAGIC spark.read
# MAGIC     .format("delta")
# MAGIC     .load("my_awesome_directory_containing_partitioned_delta_files")
# MAGIC     .where(col("action") === "StartTransaction")
# MAGIC     .show()
# MAGIC ```
# MAGIC
# MAGIC In this exercise, we'll create a filtering function that encapsulates the `where` by using the functions [year](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.year.html), [month](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.month.html), [dayofmonth](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.dayofmonth.html), [hour](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.hour.html), and [minute](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.minute.html). Eventually, the job running this logic will be triggered on a per-minute basis and will use the current time to get the previous minute's data.

# COMMAND ----------

from datetime import datetime, timezone, timedelta
from dateutil import parser
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col
from pyspark.sql import DataFrame

def read_partition(input_df: DataFrame, filter_date: datetime) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.delta_lake_silver import test_read_partition_unit

test_read_partition_unit(spark, read_partition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Read from Delta
# MAGIC Now, we'll use the selector function we just created to query the Bronze Delta storage location to fetch the data for the previous minute (relative to now). There's no code updates for you here, just run the code blocks.
# MAGIC
# MAGIC **Note:** In our other exercises, we read in pre-determined data to ensure consistent results. This exercise is different because eventually we'll use this notebook to process data from the Bronze layer in real-time.

# COMMAND ----------

bronze_input_location = working_directory.replace("silver", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC Alter the path to explore the data from the Bronze output directory. Make sure the Bronze notebook is streaming data or nothing will appear!

# COMMAND ----------

dbutils.fs.ls(f"{bronze_input_location}/output")

# COMMAND ----------

from datetime import datetime, timezone, timedelta
from dateutil import parser
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col

def read_delta_from_last_minute(now: datetime, bronze_input_location: str) -> DataFrame:
    filter_date = now - timedelta(minutes=1)
    return spark.read.\
        format("delta").\
        load(bronze_input_location).\
        transform(read_partition, filter_date)
  
df = read_delta_from_last_minute(datetime.now(tz=timezone.utc), f"{bronze_input_location}/output")

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2E Test
# MAGIC This might take a minute or so.

# COMMAND ----------

from exercise_ev_databricks_unit_tests.delta_lake_silver import test_read_delta_from_last_minute_e2e

test_read_delta_from_last_minute_e2e(read_delta_from_last_minute(datetime.now(tz=timezone.utc), f"{bronze_input_location}/output"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Event Log: Set Partitioning Columns
# MAGIC We want to eventually write two types of Delta tables: 
# MAGIC 1. Event Log: Delta Lake table partitioned by charge_point_id, year, month, and day
# MAGIC 2. Action Tables: Delta Lake tables for each OCPP action/message_type combination (e.g. "StartTransactionRequest") partitioned by charge_point_id, year, month, and day
# MAGIC
# MAGIC We'll start with (1), in this exercise.
# MAGIC
# MAGIC Similar to the exercise that we did in the Delta Lake Bronze layer, we'll set some partitioning columns by using [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html) along with the [year](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.year.html), [month](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.month.html), [day](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.dayofmonth.html) functions to create partition columns for year, month, and day based on the **write_timestamp** column (as we have in the Delta Lake Bronze exercise)

# COMMAND ----------

def set_partitioning_cols(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df

# This command takes a while. Only uncomment if you want to see what your function does. Otherwise, just run the unit test.
# display(df.transform(set_partitioning_cols))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.delta_lake_silver import test_set_partitioning_cols_unit

test_set_partitioning_cols_unit(spark, set_partitioning_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Event Log: Write to Delta Lake
# MAGIC Now that we have a function that sets some partitioning columns on our existing data, we need to partition our incoming data by those partitioning columns and write that to our output location (`working_directory + "/event_log"`).
# MAGIC
# MAGIC In this exercise, we'll write the log of events to the Delta format (using the **append** mode) and partition by charge_point_id, year, month, and day (in that order).
# MAGIC
# MAGIC Target file structure:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id=1
# MAGIC  |    |-- year=2023
# MAGIC  |    |    |-- month=5
# MAGIC  |    |    |    |-- day=23
# MAGIC  |-- charge_point_id=2
# MAGIC  |    |-- year=2023
# MAGIC  |    |    |-- month=5
# MAGIC  |    |    |    |-- day=23
# MAGIC ```

# COMMAND ----------

# Output directory
out_base_dir = f"{working_directory}/output"
print(f"Output Base Directory: {out_base_dir}")

# COMMAND ----------

def write_event_log(input_df: DataFrame, output_base_dir: str):
    ### YOUR CODE HERE
    partition_cols = [None]
    ###
    input_df.repartition(1).write.partitionBy(*partition_cols).mode("append").format("delta").save(f"{output_base_dir}/event_log")

write_event_log(df, output_base_dir=out_base_dir)



# COMMAND ----------

# MAGIC %md
# MAGIC Do we see files?

# COMMAND ----------

display(spark.createDataFrame(dbutils.fs.ls(f"{out_base_dir}/event_log/charge_point_id=5d3706d3-5866-4f52-a52c-5efca5fbb312/")))

# COMMAND ----------

# MAGIC %md
# MAGIC Can we read the files for a `charge_point_id`?

# COMMAND ----------

spark.read.format("delta").load(f"{out_base_dir}/event_log").where(col("charge_point_id") == "5d3706d3-5866-4f52-a52c-5efca5fbb312").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.delta_lake_silver import test_event_log_files_exist_e2e

test_event_log_files_exist_e2e(spark, dbutils=dbutils, out_dir=f"{out_base_dir}/event_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Transform
# MAGIC Now that we've written our data event-log-style to a Delta Lake table, we'll focus our efforts to curate Delta Lake tables specifically for each OCPP action. We'll need separate tables for each OCPP action because the tables will eventually have different schemas, courtesy of the flattening action, similar to what we've done in the Batch Processing Silver exercise.
# MAGIC
# MAGIC This exercise is about being aware that this type of transformative logic can be extracted elsewhere, so sit back and simply run the code! If you're interested to see the code behind this logic, check out [this repo](https://github.com/data-derp/exercise-ev-production-code-delta-lake/blob/main/src/exercise_ev_production_code_delta_lake/silver.py).

# COMMAND ----------

from exercise_ev_production_code_delta_lake.silver import MeterValuesRequestTransformer, StopTransactionRequestTransformer, StartTransactionRequestTransformer, StartTransactionResponseTransformer

meter_values_request_df = MeterValuesRequestTransformer().run(input_df=df)
stop_transaction_request_df = StopTransactionRequestTransformer().run(input_df=df)
start_transaction_request_df = StartTransactionRequestTransformer().run(input_df=df)
start_transaction_response_df = StartTransactionResponseTransformer().run(input_df=df)

# COMMAND ----------

# MAGIC %md
# MAGIC Interested in seeing the output?

# COMMAND ----------

meter_values_request_df.show()
stop_transaction_request_df.show()
start_transaction_request_df.show()
start_transaction_response_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write Action Tables to Delta Lake
# MAGIC We'll need a second set of tables for each OCPP action written to Delta Lake and partitioned by charge_point_id, year, month, and day.
# MAGIC
# MAGIC Target file structure:
# MAGIC ```
# MAGIC root
# MAGIC  |-- StartTransactionRequest
# MAGIC  |    |-- charge_point_id=1
# MAGIC  |    |    |-- year=2023
# MAGIC  |    |    |    |-- month=5
# MAGIC  |    |    |    |    |-- day=23
# MAGIC  |-- StartTransactionRsponse
# MAGIC  |    |-- charge_point_id=1
# MAGIC  |    |    |-- year=2023
# MAGIC  |    |    |    |-- month=5
# MAGIC  |    |    |    |    |-- day=23
# MAGIC  |-- StopTransactionRequest
# MAGIC  |    |-- charge_point_id=1
# MAGIC  |    |    |-- year=2023
# MAGIC  |    |    |    |-- month=5
# MAGIC  |    |    |    |    |-- day=23
# MAGIC  |-- MeterValuesRequest
# MAGIC  |    |-- charge_point_id=1
# MAGIC  |    |    |-- year=2023
# MAGIC  |    |    |    |-- month=5
# MAGIC  |    |    |    |    |-- day=23
# MAGIC ```

# COMMAND ----------

def write(input_df: DataFrame, output_base_dir: str):
    
    ### YOUR CODE HERE
    partition_cols = [None]
    ###

    if input_df.count() > 0:
        action, message_type = [(x.action, x.message_type) for x in input_df.limit(1).select("action", "message_type").collect()][0] # => There should just be one here
        message_type_mapping = {
            2: "Request",
            3: "Response"
        }
        output_dir = f"{output_base_dir}/{action}{message_type_mapping[message_type]}"
        print(f"Writing to {output_dir}")
        input_df.transform(set_partitioning_cols).repartition(1).write.partitionBy(*partition_cols).mode("append").format("delta").save(f"{output_base_dir}/{action}{message_type_mapping[message_type]}")
    else:
        print(f"No records. Nothing to do here! \o/")



write(meter_values_request_df, output_base_dir=out_base_dir)
write(stop_transaction_request_df, output_base_dir=out_base_dir)
write(start_transaction_request_df, output_base_dir=out_base_dir)
write(start_transaction_response_df, output_base_dir=out_base_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Making data available to consumers
# MAGIC Now that we have written our data, make your data queryable by consumers by specifying the location of your data.

# COMMAND ----------

import re

def create_database(user: str):
    db = re.sub('[^A-Za-z0-9]+', '', user)
    print(f"DB name: {db}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"USE {db}")
    return db

db = create_database(current_user)

# COMMAND ----------

def create_table(db: str, table: str, data_location: str):
    statement = f"CREATE TABLE IF NOT EXISTS {db}.{table} USING DELTA LOCATION '{data_location}';"
    print(f"Executing: {statement}")
    spark.sql(statement)

create_table(
    db=db, 
    table="delta_event_log", 
    data_location=f"{out_base_dir}/event_log"
)

create_table(
    db=db, 
    table="delta_meter_values_request", 
    data_location=f"{out_base_dir}/MeterValuesRequest"
)

create_table(
    db=db, 
    table="delta_start_transaction_request", 
    data_location=f"{out_base_dir}/StartTransactionRequest"
)

create_table(
    db=db, 
    table="delta_start_transaction_response", 
    data_location=f"{out_base_dir}/StartTransactionResponse"
)

create_table(
    db=db, 
    table="delta_stop_transaction_request", 
    data_location=f"{out_base_dir}/StopTransactionRequest"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Click on "Data" on the left panel in Databricks and find your db and table to preview the schema and data. Try running a query!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up Reminder !
# MAGIC Please don't forget to stop your stream. Go back to the end of the Bronze notebook and run the last code block to stop all the streams in that notebook