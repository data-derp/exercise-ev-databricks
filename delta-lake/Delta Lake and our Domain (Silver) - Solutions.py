# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake and our Domain (Silver)
# MAGIC In the Silver layer, we'll clean-up and harmonise our data without massive reducing the dimensions of data:
# MAGIC 1. Ingest data from the Bronze layer from the last minute
# MAGIC 2. Transform (flatten, standardise, harmonise, deduplicate)
# MAGIC 3. Write in Delta Lake to store in a dedicated storage location

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
# MAGIC ## EXERCISE: Read Data
# MAGIC In our other exercises, we read in pre-determined data to ensure consistent results. This exercise is different because eventually we'll use this notebook to process data from the Bronze layer in real-time.

# COMMAND ----------

bronze_input_location = working_directory.replace("silver", "bronze")
dbutils.fs.ls(f"{bronze_input_location}/output/year=2023/month=5/day=23/hour=15/minute=0")

# COMMAND ----------



# COMMAND ----------

from datetime import datetime, timezone, timedelta
from dateutil import parser
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col
from pyspark.sql import DataFrame

def read_partition(input_df: DataFrame, filter_date: datetime) -> DataFrame:
    return input_df.where(
        (year(col("write_timestamp")) == filter_date.year) &
        (month(col("write_timestamp")) == filter_date.month) &
        (dayofmonth(col("write_timestamp")) == filter_date.day) &
        (hour(col("write_timestamp")) == filter_date.hour) &
        (minute(col("write_timestamp")) == filter_date.minute)
    )

# COMMAND ----------

from exercise_ev_databricks_unit_tests.delta_lake_silver import test_read_partition_unit

test_read_partition_unit(spark, read_partition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Read from Avro

# COMMAND ----------

from datetime import datetime, timezone, timedelta
from dateutil import parser
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, col

def read_avro(now: datetime, bronze_input_location: str) -> DataFrame:
    filter_date = now - timedelta(minutes=1)
    return spark.read.\
        format("avro").\
        load(bronze_input_location).\
        transform(read_partition, filter_date)
  
# df = read_partition(datetime.now(tz=timezone.utc), f"{bronze_input_location}/output")
df = read_avro(parser.parse("2023-05-23T15:00:00+00:00"), f"{bronze_input_location}/output/")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2E Test
# MAGIC This might take a minute or so (not because reading Avro is slow, but because one of the assertions counts the number of records which is a shuffle).

# COMMAND ----------

def test_read_avro_e2e(input_df: DataFrame, **kwargs):
    result = input_df
    result.show()
    result.repartition(1)
    result_count = result.count()
    expected_count = 1
    assert result_count >= expected_count, f"expected >= {expected_count}, but got {result_count}"

# test_read_avro_e2e(read_avro(datetime.now(tz=timezone.utc), f"{bronze_input_location}/output"))
test_read_avro_e2e(read_avro(parser.parse("2023-05-23T15:00:00+00:00"), f"{bronze_input_location}/output/year=2023/month=5/day=23/hour=15"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Set Partitioning Columns
# MAGIC Similar to the exercise that we did in the Delta Lake Bronze layer, we'll set some partitioning columns by using [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html) along with the [year](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.year.html), [month](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.month.html), [day](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.dayofmonth.html) functions to create partition columns for year, month, and day based on the **write_timestamp** column (as we have in the Delta Lake Bronze exercise)

# COMMAND ----------

############### SOLUTION ##############
def set_partitioning_cols(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df.\
        withColumn("year", year(col("write_timestamp"))). \
        withColumn("month", month(col("write_timestamp"))). \
        withColumn("day", dayofmonth(col("write_timestamp")))
    ###

display(df.transform(set_partitioning_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write Event Log to Delta Lake
# MAGIC Some of our consumers want to view the event logs for a single charger. For this reason, we'll write our data as-is to Delta Lake and partition by year, month, day. You might be wondering, why can't our consumers just query the Bronze layer (Avro)? That's because (1) the Bronze layer, while it has the same data partitioned similarly to how we want, this layer serves the functional responsibility to ingest and stage our data and (2) Avro is great for writing, but not optimised for reading (Delta Lake - and Parquet for that matter - are much more optimised for it).
# MAGIC
# MAGIC In this exercise, we'll write the log of events to the Delta format (using the **append** mode) and partition by charge_point_id, year, month, and day (in that order).

# COMMAND ----------

# Output directory
out_base_dir = f"{working_directory}/output"
print(f"Output Base Directory: {out_base_dir}")

# COMMAND ----------

################ SOLUTION ################

def write_event_log(input_df: DataFrame, output_base_dir: str):
    ### YOUR CODE HERE
    partition_cols = ["charge_point_id", "year", "month", "day"]
    ###
    df_with_partition_columns.repartition(1).write.partitionBy(*partition_cols).mode("append").format("delta").save(f"{output_base_dir}/event_log")

write_event_log(df, output_base_dir=event_log_out_dir)



# COMMAND ----------

display(spark.createDataFrame(dbutils.fs.ls(f"{event_log_out_dir}/charge_point_id=5d3706d3-5866-4f52-a52c-5efca5fbb312/")))

# COMMAND ----------

spark.read.format("delta").load(event_log_out_dir).where(col("charge_point_id") == "5d3706d3-5866-4f52-a52c-5efca5fbb312").show()

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

meter_values_request_df.show()
stop_transaction_request_df.show()
start_transaction_request_df.show()
start_transaction_response_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write Action Tables to Delta Lake
# MAGIC Because 

# COMMAND ----------

############ SOLUTION #############
def write(input_df: DataFrame, output_base_dir: str):
    ### YOUR CODE HERE
    partition_cols = ["charge_point_id", "year", "month", "day"]
    ###

    action, message_type = [(x.action, x.message_type) for x in input_df.limit(1).select("action", "message_type").collect()][0] # => There should just be one here
    message_type_mapping = {
        2: "Request",
        3: "Response"
    }
    output_dir = f"{output_base_dir}/{action}{message_type_mapping[message_type]}"
    print(f"Writing to {output_dir}")
    input_df.transform(set_partitioning_cols).repartition(1).write.partitionBy(*partition_cols).mode("append").format("delta").save(f"{output_base_dir}/{action}{message_type_mapping[message_type]}")



write(meter_values_request_df, output_base_dir=out_base_dir)
write(stop_transaction_request_df, output_base_dir=out_base_dir)
write(start_transaction_request_df, output_base_dir=out_base_dir)
write(start_transaction_response_df, output_base_dir=out_base_dir)

# COMMAND ----------



# COMMAND ----------

display(spark.createDataFrame(dbutils.fs.ls(f"{out_base_dir}/MeterValuesRequest")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: View your Data in Databricks SQL
# MAGIC Now that we have written our data, make your data queryable by consumers.

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
    statement = f"CREATE TABLE IF NOT EXISTS {db}.{table} USING PARQUET LOCATION '{data_location}';"
    print(f"Executing: {statement}")
    spark.sql(statement)



# COMMAND ----------

create_table(
    db=db, 
    table="delta_meter_values_request", 
    data_location=f"{out_base_dir}/MeterValuesRequest"
)
