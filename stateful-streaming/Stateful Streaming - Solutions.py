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



# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col,lit

url = "https://raw.githubusercontent.com/kelseymok/charge-point-live-status/main/ocpp_producer/data/1683036538.json"
pandas_df = pd.read_json(url, orient='records')
pandas_df["index"] = pandas_df.index
mock_data_df = spark.createDataFrame(pandas_df)


# COMMAND ----------

print(mock_data_df)

# COMMAND ----------

display(mock_data_df.select("index"))

# COMMAND ----------

display(mock_data_df)

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
joined_df = raw_stream_data.\
    join(mock_data_df, raw_stream_data.value == mock_data_df.index, 'left').\
    drop("timestamp").\
    drop("index")

# I really hope that this joins properly, could not test.

# COMMAND ----------

# Extract fields from the nested body column and get that in the extracted dataframe

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, ArrayType, DoubleType, LongType

body_schema = StructType([
    StructField("connector_id", IntegerType(), True),
    StructField("error_code", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("info", IntegerType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("vendor_error_code", IntegerType(), True),
])

exploded_df = joined_df.withColumn("new_body", from_json(col("body"), schema=body_schema))




# COMMAND ----------


# Let us now filter the exploded dataframe above to get status and timestamp from the body field
# We are also dropping fields body, new_body, value etc which are not required
# The resulting dataframe essentially will just have 3 field remaining i.e charge_point_id, status and timestamp

from pyspark.sql.functions import col, expr, to_timestamp
import pyspark.sql.functions as F

filtered_df = exploded_df \
    .withColumn("status", expr("new_body.status"))\
    .withColumn("timestamp", to_timestamp(col("new_body.timestamp")))\
    .drop("body")\
    .drop("new_body")\
    .drop("value")\
    .drop("action")\
    .drop("message_type")


# COMMAND ----------

from pyspark.sql.functions import from_json, window

# This is the real meat of the logic where we groupBy charge_point_id, status and Time Window
# We then aggregate it to get the status count in a particular time window
agg_df = filtered_df\
    .withWatermark("timestamp", "10 minute") \
    .groupBy(col("charge_point_id").alias("Charge Point Id"),
             col("status").alias("Latest Status"),
             window(col("timestamp"), "5 minute").alias("Time Window"))\
    .agg(F.count(col("status")).alias("Status Count"))


# COMMAND ----------

query = agg_df \
    .writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("counts")\
    .option('truncate', 'false') \
    .start()


# COMMAND ----------

# MAGIC %sql select `Time Window`, `Charge Point Id`, `Status Count` from counts;

# COMMAND ----------

# MAGIC %sql select date_format(`Time Window`.end, "MMM-dd HH:mm") as time from counts

# COMMAND ----------



display(agg_df)

# COMMAND ----------

# From here, we can use from_json to unpack the body, pull out the `status` 

# COMMAND ----------

# Resulting DF/state store should be "charge_point_id, status". We can have a little paragraph in here about what kind of window we're choosing and why.
