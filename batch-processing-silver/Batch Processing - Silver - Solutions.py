# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Layer
# MAGIC 
# MAGIC In the last exercise, we took our data and partitioned them by YYYY, MM, DD, and HH. In this exercise, we'll take our first step towards curation and cleanup by:
# MAGIC * Unpacking strings containing json to JSON
# MAGIC * Flattening our data (unpack nested structures and bring to top level)
# MAGIC * Casting ambiguous columns to types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "batch_processing_silver"

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
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the bronze layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_bronze_ingest")
print(input_dir)


# COMMAND ----------

# MAGIC %md
# MAGIC **Note** this might take a while! Grab a coffee.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
# df = read_parquet(f"{input_dir}/output/year=2023")
df = read_parquet(f"{input_dir}/output/")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Filter

# COMMAND ----------

def start_transaction_request_filter(input_df: DataFrame):
    return input_df.filter((input_df.action == "StartTransaction") & (input_df.message_type == 2))

display(df.transform(start_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Unpack JSON

# COMMAND ----------

from pyspark.sql.functions import from_json, col

def start_transaction_request_body_schema():
    schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    
    return schema

def start_transaction_request_unpack_json(input_df: DataFrame):
    return input_df.withColumn("new_body",from_json(col("body"), start_transaction_request_body_schema()))

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Flatten

# COMMAND ----------

def start_transaction_request_flatten(input_df: DataFrame):
    return input_df.\
        withColumn("connector_id", input_df.new_body.connector_id).\
        withColumn("id_tag", input_df.new_body.connector_id).\
        withColumn("meter_start", input_df.new_body.meter_start).\
        withColumn("timestamp", input_df.new_body.timestamp).\
        withColumn("reservation_id", input_df.new_body.reservation_id).\
        drop("new_body")


display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Response

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Filter

# COMMAND ----------

def start_transaction_response_filter(input_df: DataFrame):
    return input_df.filter((input_df.action == "StartTransaction") & (input_df.message_type == 3))

display(df.transform(start_transaction_response_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Unpack JSON

# COMMAND ----------

def start_transaction_response_body_schema():
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])

    return schema
    

def start_transaction_response_unpack_json(input_df: DataFrame):
    return input_df.withColumn("new_body",from_json(col("body"), start_transaction_response_body_schema()))

display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Flatten

# COMMAND ----------

def start_transaction_response_flatten(input_df: DataFrame):
    return input_df.\
        withColumn("transaction_id", input_df.new_body.transaction_id).\
        withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status).\
        withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag).\
        withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date).\
        drop("new_body")
  
display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StopTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Filter

# COMMAND ----------

def stop_transaction_request_filter(input_df: DataFrame):
    return input_df.filter((input_df.action == "StopTransaction") & (input_df.message_type == 2))

display(df.transform(stop_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Unpack JSON

# COMMAND ----------

from pyspark.sql.types import ArrayType

def stop_transaction_body_schema():
    return StructType([
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
    ])
    
def stop_transaction_request_unpack_json(input_df: DataFrame):
    return input_df.withColumn("new_body",from_json(col("body"), stop_transaction_body_schema()))


display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Flatten

# COMMAND ----------

def stop_transaction_request_flatten(input_df: DataFrame):
    return input_df.\
        withColumn("meter_stop", input_df.new_body.meter_stop).\
        withColumn("timestamp", input_df.new_body.timestamp).\
        withColumn("transaction_id", input_df.new_body.transaction_id).\
        withColumn("reason", input_df.new_body.reason).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn("transaction_data", input_df.new_body.transaction_data).\
        drop("new_body")

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process MeterValues Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Filter

# COMMAND ----------

def meter_values_request_filter(input_df: DataFrame):
    return input_df.filter((input_df.action == "MeterValues") & (input_df.message_type == 2))

display(df.transform(meter_values_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Unpack JSON

# COMMAND ----------

def meter_values_request_unpack_json(input_df: DataFrame):
    sampled_value_schema = StructType([
        StructField("value", StringType()),
        StructField("context", StringType()),
        StructField("format", StringType()),
        StructField("measurand", StringType()),
        StructField("phase", StringType()),
        StructField("unit", StringType()),
    ])

    meter_value_schema = StructType([
        StructField("timestamp", StringType()),
        StructField("sampled_value", ArrayType(sampled_value_schema)),
    ])

    body_schema = StructType([
        StructField("connector_id", IntegerType()),
        StructField("transaction_id", IntegerType()),
        StructField("meter_value", ArrayType(meter_value_schema)),
    ])
    
    return input_df.withColumn("new_body", from_json(col("body"), body_schema))

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Flatten

# COMMAND ----------

from pyspark.sql.functions import explode, to_timestamp, round
from pyspark.sql.types import DoubleType


def meter_values_request_flatten(input_df: DataFrame):
    return input_df. \
        select("*", explode("new_body.meter_value").alias("meter_value")). \
        select("*", explode("meter_value.sampled_value").alias("sampled_value")). \
        withColumn("timestamp", to_timestamp(col("meter_value.timestamp"))).\
        withColumn("measurand", col("sampled_value.measurand")).\
        withColumn("phase", col("sampled_value.phase")).\
        withColumn("value", round(col("sampled_value.value").cast(DoubleType()),2)).\
        select("message_id", "message_type", "charge_point_id", "action", "write_timestamp", col("new_body.transaction_id").alias("transaction_id"), "timestamp", "measurand", "phase", "value")

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json).transform(meter_values_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

out_dir = f"{working_directory}/output/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Request to Parquet

# COMMAND ----------

df.\
    transform(start_transaction_request_filter).\
    transform(start_transaction_request_unpack_json).\
    transform(start_transaction_request_flatten).\
    write.\
    mode("overwrite").\
    parquet(f"{out_dir}/StartTransactionRequest")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Response to Parquet

# COMMAND ----------

df.\
    transform(start_transaction_response_filter).\
    transform(start_transaction_response_unpack_json).\
    transform(start_transaction_response_flatten).\
    write.\
    mode("overwrite").\
    parquet(f"{out_dir}/StartTransactionResponse")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StopTransaction Request to Parquet

# COMMAND ----------

df.\
    transform(stop_transaction_request_filter).\
    transform(stop_transaction_request_unpack_json).\
    transform(stop_transaction_request_flatten).\
    write.\
    mode("overwrite").\
    parquet(f"{out_dir}/StopTransactionRequest")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write MeterValues Request to Parquet
# MAGIC TODO: Why does this take 32.22 seconds??

# COMMAND ----------

df.\
    transform(meter_values_request_filter).\
    transform(meter_values_request_unpack_json).\
    transform(meter_values_request_flatten).\
    write.\
    mode("overwrite").\
    parquet(f"{out_dir}/MeterValuesRequest")

# COMMAND ----------

dbutils.fs.ls(f"{out_dir}/MeterValuesRequest")

# COMMAND ----------


