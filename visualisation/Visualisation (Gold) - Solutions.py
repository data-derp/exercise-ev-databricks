# Databricks notebook source
# MAGIC %md
# MAGIC # Visualisations
# MAGIC In this exercise, we'll take our data from the Gold layer and create a visualisation. We want to show a distribution of charge dispensed along with the mean, median and range.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisations with Plotly
# MAGIC Before we get started on the exercise, we'll quickly review [Plotly](https://plotly.com/), which is a common basic tool for Data Visualisations. 
# MAGIC
# MAGIC Examples from [Plotly](https://plotly.com/python/getting-started/)

# COMMAND ----------

import plotly.express as px

# We see that set labels on the x-axis and the values on the y-axis.
fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
fig.write_html('first_figure.html', auto_open=True)
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

exercise_name = "visualisation"

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
# MAGIC ## Read Data from Gold Layer
# MAGIC Let's read the parquet files that we created in the bronze layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_gold")
print(input_dir)


# COMMAND ----------

dbutils.fs.ls(f"{input_dir}/output/cdr")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(f"{input_dir}/output/cdr")

display(df)


# COMMAND ----------

############ SOLUTION #############

import plotly.express as px
df_in_pandas = df.toPandas()
fig = px.histogram(df_in_pandas, x="total_energy")
fig.show()

# COMMAND ----------

############ SOLUTION #############

import plotly.express as px

fig = px.box(df_in_pandas, y="total_energy")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Topic: Dashboards (Bonus)
# MAGIC Everyone talks about Dashbaords. It's possible to go beyond our singular visualisations and compile them into a dashboard for someone to read. There are a variety of tools that exist, such as Dash, which works great with Plotly, and [StreamLit](https://streamlit.io/).
# MAGIC
# MAGIC When working with Dash, independent applications can be built and deployed to production environments. Dash applications can also run in-line in notebooks and also [within Databricks](https://medium.com/plotly/building-plotly-dash-apps-on-a-lakehouse-with-databricks-sql-advanced-edition-4e1015593633).
