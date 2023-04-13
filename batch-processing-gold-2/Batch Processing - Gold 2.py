# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Gold 2
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

exercise_name = "batch_processing_gold_1"

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

# Match StartTransaction Requests and Responses
def match_start_transaction_requests_with_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.\
        join(join_df, input_df.message_id == join_df.message_id, "left").\
        select(
            input_df.charge_point_id.alias("charge_point_id"), 
            join_df.transaction_id.alias("transaction_id"), 
            input_df.meter_start.alias("meter_start"), 
            input_df.timestamp.alias("start_timestamp")
        )
    ###
    
display(start_transaction_request_df.transform(match_start_transaction_requests_with_responses, start_transaction_response_df))

# COMMAND ----------

# Join Stop Transaction Requests and StartTransaction Responses, matching on transaction_id (left join)
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
        start_transaction_request_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_response_df)
    )
)

# COMMAND ----------

# Calculate the total_time (withColumn, cast, maths)
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
        start_transaction_request_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_response_df)
    ).\
    transform(calculate_total_time)
)

# COMMAND ----------

# Calculate total_energy (withColumn, cast)

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df \
        .withColumn("total_energy", col("meter_stop") - col("meter_start")) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    ###

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_request_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_response_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

# Calculate total_parking_time (explode, filter, window, groupBy)

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

# Join and Shape (left join, select)
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
        start_transaction_request_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_response_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------


