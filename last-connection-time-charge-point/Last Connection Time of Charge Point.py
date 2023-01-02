# Databricks notebook source
# MAGIC %md
# MAGIC # Last Connection Time of Charge Points
# MAGIC After the Charge Point has registered itself with the CSMS, it sends OCPP messages to the CSMS (for example: StartTransaction, StopTransaction, MeterValues, etc). If there are no OCPP-messages sent during the heartbeat interval, it sends a Heartbeat message to denote its responsiveness. 
# MAGIC 
# MAGIC When was the last connection time of each of our Charge Points?
# MAGIC 
# MAGIC We can find out when it was last connected by finding the timestamp of the most recent message from any OCPP action for that Charge Point.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contents
# MAGIC * Set up this Notebook
# MAGIC * Data Ingestion
# MAGIC   * Read Data with a Schema
# MAGIC * Data Transformation
# MAGIC   * Time Conversion
# MAGIC   * Windows and Rows
# MAGIC   * Cleanup
# MAGIC * Data Visualisation
# MAGIC   * Visualise it!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

exercise_name = "last_connection_time_of_charge_point"

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
# MAGIC ## DATA INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Read Data
# MAGIC 
# MAGIC #### Context
# MAGIC The first step in handling data in Spark (especially if the data exists already) is to read that data into Spark as a DataFrame. Data can come in various formats (CSV, JSON, Parquet, Avro, Delta Lake, etc) and Spark has several API methods to read these specific formats.
# MAGIC 
# MAGIC | Format | Example |
# MAGIC | --- | --- |
# MAGIC | CSV | df = spark.read.format("csv").load("/tmp/data.csv") |
# MAGIC | JSON | df = spark.read.format("json").load("/tmp/data.json") |
# MAGIC | Parquet | df = spark.format("parquet").load("/tmp/data.parquet") |
# MAGIC 
# MAGIC There are additional options that can be provided:
# MAGIC ```
# MAGIC spark.read.format("csv") \
# MAGIC       .option("header", True) \
# MAGIC       .option("inferSchema", True) \
# MAGIC       .option("delimiter", ",") \
# MAGIC       .load("/tmp/data.csv")
# MAGIC ```
# MAGIC 
# MAGIC In this example, we are reading a CSV file, denoting that there is a header row, we expect our delimiter to be a comma, and we would like Spark to simply guess (infer) what the schema of the file is. Letting Spark infer the schema *works* but doesn't necessarily yield the most accurate reads.
# MAGIC 
# MAGIC It is recommended to define a **schema** and supply that at the time of reading:
# MAGIC ```   
# MAGIC custom_schema = StructType([
# MAGIC   StructField("animal", StringType(), True),
# MAGIC   StructField("count", IntegerType(), True),
# MAGIC ])
# MAGIC       
# MAGIC spark.read.format("csv") \
# MAGIC   .option("header", True) \
# MAGIC   .option("delimiter", ",") \
# MAGIC   .schema(custom_schema) \
# MAGIC   .load("/tmp/data.csv")
# MAGIC ```
# MAGIC 
# MAGIC It's worth noting that Spark (at the time of writing) cannot read directly from a URL, only File Systems. This works great when your data is in your local filestore, HDFS, or blob storage like AWS S3, but not when you need to pull fresh data remotely.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go!
# MAGIC In this Exercise, we'll learn how to read data into Spark and create a DataFrame from it. 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Recall that we need to download the data to our local directory in order for Spark to access it. Let's download the file (using one of our helper functions) that we'll use as part of this exercise.

# COMMAND ----------

url = "https://github.com/data-derp/exercise-ev-databricks/blob/main/data_generator/out/data.csv?raw=true"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reflect
# MAGIC What kind of file are we working with? What format?

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Your Turn**: Use the `spark.read.format("csv")` function to read in the file that we've just downloaded. Don't forget to include the delimiter, schema, and the fact that there is a header.
# MAGIC 
# MAGIC Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  
# MAGIC  ```

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def create_dataframe(filepath: str) -> DataFrame:
    ### YOUR CODE HERE ###
    custom_schema = None

    df = None
    return df
    ###

df = create_dataframe(filepath)

# COMMAND ----------

def test_create_dataframe():
    result = create_dataframe(filepath)
    assert result is not None
    assert result.columns == ['charge_point_id', 'write_timestamp', 'action', 'body']
    assert result.count() == 484
    print("All tests pass! :)")
    
test_create_dataframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Time Conversion

# COMMAND ----------

# MAGIC %md
# MAGIC In order to find the most recent message from each Charge Point, we need to sort each of the Charge Point messages by the `write_timestamp` field. However, note that the `write_timestamp` field is a String:
# MAGIC 
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC ```
# MAGIC 
# MAGIC Therefore, a sort on `write_timestamp` will yield an alphabetical sorting as opposed to a proper time-based sorting.

# COMMAND ----------

# MAGIC %md
# MAGIC Using the `withColumn` and `to_timestamp` methods, add a new column to the DataFrame called `converted_timestamp` which is of `timestamp` type

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def convert_to_timestamp(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.transform(convert_to_timestamp).show()

# COMMAND ----------

from pyspark.sql.types import TimestampType
import pandas as pd

def test_convert_to_timestamp():    
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:30:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}"
        },
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:32:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}"
        },
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:34:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}"
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("write_timestamp", StringType()),
            StructField("action", StringType()),
            StructField("body", StringType()),
        ]))

    input_df = spark.createDataFrame(input_pandas)
    result = input_df.transform(convert_to_timestamp)
    assert result.count() == 3
    assert result.columns == ["charge_point_id", "write_timestamp", "action", "body", "converted_timestamp"]
    
    expected_schema = StructType([
        StructField('charge_point_id', StringType(), True), 
        StructField('write_timestamp', StringType(), True), 
        StructField('action', StringType(), True), 
        StructField('body', StringType(), True), 
        StructField('converted_timestamp', TimestampType(), True)
    ])
    assert result.schema == expected_schema
    print("All tests pass! :)")
    
test_convert_to_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC We can now sort the entire dataframe by timestamp, with the most recent messages at the top!

# COMMAND ----------

from pyspark.sql.functions import col

df.transform(convert_to_timestamp).sort(col("converted_timestamp").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Windows and Rows
# MAGIC In reality, we actually need the most recent message PER distinct Charge Point ID. We can use a GroupBy statement, Windowing, OrderBy, and Sorting in order to achieve this. 

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's note the unique Charge Points available to us in the data:

# COMMAND ----------

from pyspark.sql.functions import *

df.select("charge_point_id").distinct().sort(col("charge_point_id").asc()).show()

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import row_number

def most_recent_message_of_charge_point(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.transform(convert_to_timestamp).transform(most_recent_message_of_charge_point).show()

# COMMAND ----------

from dateutil.parser import parse
def test_most_recent_message_of_charge_point():
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:30:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:30:17.000345+00:00")
        },
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:32:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:32:17.000345+00:00")
        },
        {
            "charge_point_id": "AL2000",
            "write_timestamp": "2022-10-02T15:34:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:34:17.000345+00:00"),
        },
        {
            "charge_point_id": "AL2000",
            "write_timestamp": "2022-10-02T15:36:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:36:17.000345+00:00"),
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("write_timestamp", StringType()),
            StructField("action", StringType()),
            StructField("body", StringType()),
            StructField("converted_timestamp", TimestampType()),
        ]))

    input_df = spark.createDataFrame(input_pandas)
    result = input_df.transform(most_recent_message_of_charge_point)
    assert result.count() == 2
    assert result.columns == ["charge_point_id", "write_timestamp", "action", "body", "converted_timestamp", "rn"]
    
test_most_recent_message_of_charge_point()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Cleanup
# MAGIC Hooray! We now have the list of Charge Points with the timestamp of their most recent message. However, did you notice that there's this weird `rn` column leftover from the last transformation?

# COMMAND ----------

df.transform(convert_to_timestamp).transform(most_recent_message_of_charge_point).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's clean that up. Use the `drop` function to remove the `rn` column

# COMMAND ----------

def cleanup(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###

# COMMAND ----------

from pyspark.sql.types import IntegerType

def test_cleanup():
    input_pandas = pd.DataFrame([
        {
            "charge_point_id": "AL1000",
            "write_timestamp": "2022-10-02T15:32:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:32:17.000345+00:00"),
            "rn": 1
        },
        {
            "charge_point_id": "AL2000",
            "write_timestamp": "2022-10-02T15:36:17.000345+00:00",
            "action": "Heartbeat",
            "body": "{}",
            "converted_timestamp": parse("2022-10-02T15:36:17.000345+00:00"),
            "rn": 1
        },
    ])

    input_df = spark.createDataFrame(
        input_pandas,
        StructType([
            StructField("charge_point_id", StringType()),
            StructField("write_timestamp", StringType()),
            StructField("action", StringType()),
            StructField("body", StringType()),
            StructField("converted_timestamp", TimestampType()),
            StructField("rn", IntegerType())
        ]))

    input_df = spark.createDataFrame(input_pandas)
    result = input_df.transform(cleanup)
    assert result.count() == 2
    assert result.columns == ["charge_point_id", "write_timestamp", "action", "body", "converted_timestamp"]
    
test_cleanup()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## All together now!

# COMMAND ----------

final_df = df.transform(convert_to_timestamp).\
    transform(most_recent_message_of_charge_point).\
    transform(cleanup)
final_df.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType

def test_final():
    assert final_df.count() == 5
    assert final_df.select("charge_point_id").
    assert result.columns == ["charge_point_id", "write_timestamp", "action", "body", "converted_timestamp"]
    
test_cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA VISUALISATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Visualise It!
# MAGIC Recall that our task was to show the latest timestamp of each Charge Point. In this case, it doesn't make too much sense to introduce a colourful graph; in fact, a list or table would be just fine. Looking at our data, the two relevant columns are `charge_point_id` and `write_timestamp`.
# MAGIC 
# MAGIC Use the [display function](https://docs.databricks.com/notebooks/visualizations/index.html) to show only the `charge_point_id` and `write_timestamp` columns

# COMMAND ----------

### YOUR CODE HERE

###
