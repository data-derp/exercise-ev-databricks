# Databricks notebook source
# MAGIC %md
# MAGIC # Stateful Streaming
# MAGIC
# MAGIC ~~Active chargers in the last 5 minutes~~
# MAGIC
# MAGIC
# MAGIC It is very common for a Charge Point Operator to want to understand current status of the chargers that are reporting. In this exercise, we'll use our knowledge of Stateful streaming to report on the **Status of Chargers in the last 5 minutes**
# MAGIC
# MAGIC * ingest StatusNotification Stream
# MAGIC * unpack status
# MAGIC * ignore late data past 5 minutes
# MAGIC * write to local 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests freezegun ocpp

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

import pandas as pd
from pyspark.sql.functions import monotonically_increasing_id


url = "https://raw.githubusercontent.com/kelseymok/charge-point-live-status/main/ocpp_producer/data/1683036538.json"
pandas_df = pd.read_json(url, orient='records')
mock_data_df = spark.createDataFrame(pandas_df).withColumn("temp_index", monotonically_increasing_id())


# COMMAND ----------

print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Read from Stream
# MAGIC In this exercise, we'll read from the stream

# COMMAND ----------

# The section below creates a raw stream of data (timed) by using the rate source. 
# We then join it with our data on an index so we can leverage the behaviour of the rate source.
# It is unclear if the rate.value starts with 0 or 1 but whatever it is, it needs to match with the indexes that we have. Would be great to confirm it.


### YOUR CODE HERE
streaming_format = "rate"
###
raw_stream_data = (
    spark.readStream.format(streaming_format)
    .option("rowsPerSecond", 10)
    .load()
)

# This is just data setup, not part of the exercise
raw_stream_data.\
    join(mock_data_df, raw_stream_data.value == mock_data_df.temp_index, 'left').\
    drop("timestamp").\
    drop("temp_index")

## I really hope that this joins properly, could not test.

# COMMAND ----------

# From here, we can use from_json to unpack the body, pull out the `status` 

# COMMAND ----------

# Resulting DF/state store should be "charge_point_id, status". We can have a little paragraph in here about what kind of window we're choosing and why.
