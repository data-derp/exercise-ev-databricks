# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "batch_processing_silver"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

out_dir = f"{working_directory}/output/"

# COMMAND ----------

import re
db = re.sub('[^A-Za-z0-9]+', '', current_user)
print(f"DB name: {db}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

tables = [
    {
        "meter_values_request": "MeterValuesRequest",
    }
]

for table_name, dir_name in tables:
    statement = f"CREATE TABLE {db}.{table_name} USING PARQUET LOCATION '{out_dir}/{dir_name}/';"
    spark.sql(statement)


