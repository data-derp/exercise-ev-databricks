# Databricks notebook source
# MAGIC %md
# MAGIC # Visualisations
# MAGIC In this exercise, we'll take our data from the Gold layer and create a visualisation. We want to show a distribution of charge dispensed along with the mean, median and range.
# MAGIC
# MAGIC There are many tools that we can use to generate Visualisations. This exercise will explore visualisation creation using:
# MAGIC * Databricks Graphs
# MAGIC * Plotly

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
# MAGIC Let's read the parquet files that we created in the Gold layer!

# COMMAND ----------

input_dir = working_directory.replace(exercise_name, "batch_processing_gold")
print(input_dir)


url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-gold-2/output/cdr/part-00000-tid-8188744955325376871-3934a431-560c-4ff0-b57c-88baab52939b-566-1-c000.snappy.parquet"

filepath = helpers.download_to_local_dir(url)


# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(filepath)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Visualisations with Databricks
# MAGIC Let's start with the easy one. Databricks comes with Visualisation and Dashboarding capabilities. When we `display` our DataFrame in Databricks, we have the possibility to create a Visualisation from it.
# MAGIC
# MAGIC 1. Click the `+` button next to the display `Table`.
# MAGIC
# MAGIC ![databricks-click-visualisation.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-click-visualisation.png?raw=true)
# MAGIC
# MAGIC
# MAGIC 2. Then, use the Visualisation Editor to create a Histogram:
# MAGIC ![databricks-visualisation-editor-histogram.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-visualisation-editor-histogram.png?raw=true)
# MAGIC
# MAGIC 3. The resulting Histogram should look as follows:
# MAGIC ![databricks-histogram-total-parking-time.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-histogram-total-parking-time.png?raw=true)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Try playing with the bin size. How does the graph change? How can this graph be interpreted?

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks also has Dashboarding capabilities! Click the drop down arrow on the Histogram you just created and click **Add to Dashboard**.
# MAGIC
# MAGIC ![databricks-histogram-add-to-dashboard.png](https://github.com/data-derp/exercise-ev-databricks/blob/main/visualisation/assets/databricks-histogram-add-to-dashboard.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC Feel free to try this with other fields in the DataFrame!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisations with Plotly
# MAGIC Outside of Databricks, there are many other visualisation tools. One of the more customisable ones is called Plotly. Before we get started on the exercise, we'll quickly review [Plotly](https://plotly.com/).
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
# MAGIC
# MAGIC ### EXERCISE: Histograms
# MAGIC Plot the `total_energy` in a [Histogram](https://plotly.com/python/histograms/) to show the distribution of the energy dispensed across all of our data. Note: Plotly can't use Spark DataFrames but rather Pandas DataFrames (convert the Spark DataFrame to a PandasDataFrame using the [toPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html) function).

# COMMAND ----------

import plotly.express as px

### YOUR CODE HERE
fig = px.histogram(None, x=None)
###

fig.show()

# COMMAND ----------

############ SOLUTION #############
import plotly.express as px

### YOUR CODE HERE
fig = px.histogram(df.toPandas(), x="total_energy")
###

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC * What kind of information can you extract from this plot? 
# MAGIC * Is this the most effective way to tell a story about this data?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### EXERCISE: Box Plots
# MAGIC Plot the `total_energy` in a [Box Plot](https://plotly.com/python/box-plots/) to show the distribution of the energy dispensed across all of our data (similar to our Histogram). Note: Plotly can't use Spark DataFrames but rather Pandas DataFrames (convert the Spark DataFrame to a PandasDataFrame using the [toPandas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html) function).

# COMMAND ----------

import plotly.express as px

### YOUR CODE HERE
fig = px.box(None, y=None)
###

fig.show()

# COMMAND ----------

############ SOLUTION #############

import plotly.express as px

fig = px.box(df.toPandas(), y="total_energy")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflect
# MAGIC * What kind of information can you extract from this plot? 
# MAGIC * Is this the most effective way to tell a story about this data? 
# MAGIC * Why are Box plots more informative than Histograms?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Topic: Dashboards (Bonus)
# MAGIC Everyone talks about Dashbaords. It's possible to go beyond our singular visualisations and compile them into a dashboard for someone to read. There are a variety of tools that exist, such as Dash, which works great with Plotly, and [StreamLit](https://streamlit.io/).
# MAGIC
# MAGIC When working with Dash, independent applications can be built and deployed to production environments. Dash applications can also run in-line in notebooks and also [within Databricks](https://medium.com/plotly/building-plotly-dash-apps-on-a-lakehouse-with-databricks-sql-advanced-edition-4e1015593633).
