# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC When creating long-term storage for analytical use cases, the first step is to ingest data for the source, with a shape as close as possible to the original shape. As the first step in our data processing journey, this allows us to 
# MAGIC 1. create a "checkpoint" or "save zone" so that we can more easily debug issues and determine if there were issues at this step or downstream
# MAGIC 2. replay data to downstream steps in the case that there is an error or interruption (data is idempotent)
# MAGIC 
# MAGIC In this exercise, we will:
# MAGIC * ingest the raw data in a single pull
# MAGIC * convert the data to parquet format (a format good for writing)
# MAGIC * partition the data by YYYY > MM > DD > HH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "batch_processing_bronze_ingest"

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
# MAGIC ### Read OCPP Data
# MAGIC We've done this a couple of times before! Run the following cells to download the data to local storage and create a DataFrame from it.

# COMMAND ----------

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        StructField("message_id", StringType(), True),
        StructField("message_type", IntegerType(), True),
        StructField("charge_point_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("write_timestamp", StringType(), True),
        StructField("body", StringType(), True),
    ])
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("escape", "\\") \
        .schema(custom_schema) \
        .load(filepath)
    return df
    
df = create_dataframe(filepath)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Create Partition Columns
# MAGIC We want to, at the end of all of the exercises, write all our data to Parquet format partitioned YYYY, MM, DD, and HH (nested, in that order). For that to happen, we'll need to create new columns `year`, `month`, `day`, and `hour` from the `write_timestamp` column.

# COMMAND ----------

############### SOLUTION ##################
from pyspark.sql.functions import to_timestamp, col, month, dayofmonth, hour, year

def create_partition_columns(input_df: DataFrame) -> DataFrame:
    input_df_converted_timestamp = input_df.withColumn("write_timestamp", to_timestamp(col("write_timestamp")))
    ### YOUR CODE HERE ###
    return input_df_converted_timestamp. \
        withColumn("write_timestamp", to_timestamp(col("write_timestamp"))). \
        withColumn("year", year(col("write_timestamp"))). \
        withColumn("month", month(col("write_timestamp"))). \
        withColumn("day", dayofmonth(col("write_timestamp"))). \
        withColumn("hour", hour(col("write_timestamp")))
    ###
    
display(df.transform(create_partition_columns))
    
    


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, month, dayofmonth, hour, year

def create_partition_columns(input_df: DataFrame) -> DataFrame:
    input_df_converted_timestamp = input_df.withColumn("write_timestamp", to_timestamp(col("write_timestamp")))
    
    ### YOUR CODE HERE ###
    return input_df_converted_timestamp
    ###
    
display(df.transform(create_partition_columns))

# COMMAND ----------

############### SOLUTION ##################
from pyspark.sql.functions import to_timestamp, col, month, dayofmonth, hour, year

def create_partition_columns(input_df: DataFrame) -> DataFrame:
    input_df_converted_timestamp = input_df.withColumn("write_timestamp", to_timestamp(col("write_timestamp")))
    ### YOUR CODE HERE ###
    return input_df_converted_timestamp. \
        withColumn("write_timestamp", to_timestamp(col("write_timestamp"))). \
        withColumn("year", year(col("write_timestamp"))). \
        withColumn("month", month(col("write_timestamp"))). \
        withColumn("day", dayofmonth(col("write_timestamp"))). \
        withColumn("hour", hour(col("write_timestamp")))
    ###
    
display(df.transform(create_partition_columns))
    
    


# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test
# MAGIC TBD

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test
# MAGIC TBD

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Repartition, Partition, and Write
# MAGIC TBD
# MAGIC 
# MAGIC **Note** this might take a while... grab a coffee.

# COMMAND ----------

def write(input_df: DataFrame):
    input_df.\
        repartition(col("year"), col("month"), col("day"), col("hour")). \
        write. \
        partitionBy("year", "month", "day", "hour"). \
        mode("overwrite"). \
        parquet(f"{working_directory}/output/")
    
df.transform(create_partition_columns).transform(write)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's inspect what we've created by commenting in and out one at the time the following lines:

# COMMAND ----------

dbutils.fs.ls(f"{working_directory}/output/year=2023")
# dbutils.fs.ls(f"{working_directory}/output/year=2023/month=1")
# dbutils.fs.ls(f"{working_directory}/output/year=2023/month=1/day=2")
# dbutils.fs.ls(f"{working_directory}/output/year=2023/month=1/day=2/hour=2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test
# MAGIC TBD

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test
# MAGIC TBD

# COMMAND ----------


