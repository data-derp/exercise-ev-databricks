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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

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

url = "https://raw.githubusercontent.com/data-derp/exercise-ev-databricks/main/data/1679387766.csv"
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
# MAGIC  |-- message_id: string (nullable = true) 
# MAGIC  |-- message_type: integer (nullable = true) 
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  
# MAGIC  ```

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

### YOUR CODE HERE
def create_dataframe(filepath: str) -> DataFrame:
    ### YOUR CODE HERE ###
    custom_schema = None

    df = None
    return df
    ###
    
df = create_dataframe(filepath)
df.show()


# COMMAND ----------

def test_create_dataframe():
    result = create_dataframe(filepath)
    assert result is not None
    assert result.columns == ['message_id', 'message_type', 'charge_point_id', 'action', 'write_timestamp', 'body']
    result_count = result.count()
    expected_count = 46322
    assert result_count == expected_count, f"Expected {expected_count} but got {result_count}"
    print("All tests pass! :)")
    
test_create_dataframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Convert String to Timestamp

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

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_convert_to_timestamp

test_convert_to_timestamp(spark, convert_to_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now sort the entire dataframe by timestamp, with the most recent messages at the top!

# COMMAND ----------

from pyspark.sql.functions import col

df.transform(convert_to_timestamp).sort(col("converted_timestamp").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Find the most recent message per Charge Point
# MAGIC In reality, we actually need the most recent message PER distinct Charge Point ID. We can use a [GroupBy](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html) statement, [Windows](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.window.html?highlight=window#pyspark.sql.functions.window), [OrderBy](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.orderBy.html?highlight=order#pyspark.sql.DataFrame.orderBy), and [Sorting](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.Column.desc.html?highlight=desc#pyspark.sql.Column.desc) in order to achieve this. 

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's note the unique Charge Points available to us in the data:

# COMMAND ----------

from pyspark.sql.functions import *

df.select("charge_point_id").distinct().sort(col("charge_point_id").asc()).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 💡 First think about how you would do this in SQL (using GroupBy, Windows, OrderBy, and sorting) and then translate it to the Spark API

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import row_number

def most_recent_message_of_charge_point(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    return input_df
    ###
    
df.transform(convert_to_timestamp).transform(most_recent_message_of_charge_point).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_most_recent_message_of_charge_point

test_most_recent_message_of_charge_point(spark, most_recent_message_of_charge_point)

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

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_cleanup

test_cleanup(spark, cleanup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## All together now!
# MAGIC Go ahead and inspect your final dataframe!

# COMMAND ----------

final_df = df.transform(convert_to_timestamp).\
    transform(most_recent_message_of_charge_point).\
    transform(cleanup)
display(final_df)

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_final

test_final(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC * What does each row represent?
# MAGIC * Which column contains the relevant final communication time of the charger?

# COMMAND ----------


