# Databricks notebook source
# MAGIC %md
# MAGIC # View Data in Databricks SQL
# MAGIC In order to use a query engine like Databricks SQL, we must create a schema that represents the data that we'll query and points to the location of our data. This is required for all query engines, not just Databricks SQL.
# MAGIC
# MAGIC In the exercise below, we'll create a schema for our **MeterValuesRequest** data from the Silver exercise and view it in Databricks SQL by doing the following:
# MAGIC 1. Read the MeterValuesRequest data
# MAGIC 2. Create a Database in Databricks SQL
# MAGIC 3. Create a Table in Databricks SQL
# MAGIC 4. View data in Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "write_schema_to_databricks_sql"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read MeterValuesRequest Data from Silver Exercise
# MAGIC We'll start by reading in the MeterValuesRequest output data from the Silver Exercise. For your convenience, we'll read some pre-prepared data.

# COMMAND ----------

# meter_values_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/MeterValuesRequest/part-00000-tid-468425781006758111-f9d48bc3-3b4c-497e-8e9c-77cf63db98f8-207-1-c000.snappy.parquet"
meter_values_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/MeterValuesRequest/"

meter_values_request_filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Database in Databricks SQL
# MAGIC We will first choose a database in which to populate tables that point to your data. Typically, this might be a team or domain name that is relevant to your organisation. In this case, we'll create a database based on your Databricks username to prevent clashes with others who might be doing the same exercise.

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

# MAGIC %md
# MAGIC ## Create a Table in Databricks SQL
# MAGIC Now that we have a database, we can create a table that points to the location of our data (`meter_values_request_filepath`).

# COMMAND ----------

def create_table(db: str, table: str, data_location: str):
    statement = f"CREATE TABLE IF NOT EXISTS {db}.{table} USING PARQUET LOCATION '{data_location}';"
    print(f"Executing: {statement}")
    spark.sql(statement)

create_table(
    db=db, 
    table="meter_values_request", 
    data_location=meter_values_request_filepath
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Data in Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Navigate to Databricks SQL using the drop down and clicking on "Data". **Hint:** right click on the elements to open it in a new tab.
# MAGIC
# MAGIC ![navi-to-dbx-sql.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/navi-to-file-explorer.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Check that your database and table exists 
# MAGIC
# MAGIC ![table-present.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/table-present.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Clicking on the Table should show the present columns on the right pane
# MAGIC
# MAGIC ![view-columns.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/view-columns.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Navigate to the SQL Editor (where we can query our data)
# MAGIC
# MAGIC ![select-sql-editor.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/select-sql-editor.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Select your Database
# MAGIC
# MAGIC ![change-database-from-default-to-user.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/change-database-from-default-to-user.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 6. Query your data:
# MAGIC
# MAGIC ```
# MAGIC SELECT * from <YOUR DATABASE NAME>.meter_values_request;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 7. View your data! 
# MAGIC ![view-data.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/view-data-in-dbx-sql/assets/view-data.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking for a Challenge? (Bonus)
# MAGIC If you'd like to explore more of your data, feel free to try this with some of the other Silver data!

# COMMAND ----------

urls = {
    "start_transaction_request": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionRequest/part-00000-tid-9191649339140138460-0a4f58e5-1397-41cc-a6a1-f6756f3332b6-218-1-c000.snappy.parquet",
    "start_transaction_response": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionResponse/part-00000-tid-5633887168695670016-762a6dfa-619c-412d-b7b8-158ee41df1b2-185-1-c000.snappy.parquet",
    "stop_transaction_request": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StopTransactionRequest/part-00000-tid-5108689541678827436-b76f4703-dabf-439a-825d-5343aabc03b6-196-1-c000.snappy.parquet"
}



# COMMAND ----------

# MAGIC %md
# MAGIC Pick a URL...

# COMMAND ----------

### YOUR CODE HERE
file_path = helpers.download_to_local_dir(None)
###

# COMMAND ----------

# MAGIC %md
# MAGIC Create a table

# COMMAND ----------

### YOUR CODE HERE
create_table(
    db=db, 
    table=None, # Your Table Name 
    data_location=None # The file path
)
###
