import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.connect.session import DatabricksSession as SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pyspark.sql.functions as F


# Generic Imports
import json
import pandas as pd

w = WorkspaceClient(
    profile="e2demo"
)

# cluster_id = os.environ["TEST_DEFAULT_CLUSTER_ID"]
cluster_id = "0601-182128-dcbte59m"
# context = w.command_execution.create(cluster_id=cluster_id, language=compute.Language.PYTHON).result()

# text_results = w.command_execution.execute(
#     cluster_id=cluster_id,
#     context_id=context.id,
#     language=compute.Language.PYTHON,
#     # command="print(1)"
#     command='spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10")'
# ).result()

# print(text_results)
# data_sparkdf = text_results.results.data
# print(type(data_sparkdf)) # this returns only the metadata of returned DF

config = WorkspaceClient(
    profile="e2demo",
    cluster_id=cluster_id
).config
spark = SparkSession.builder.sdkConfig(config).getOrCreate()
df = spark.read.table("samples.nyctaxi.trips")
pdf = df.toPandas()
print(pdf)
# cleanup
# w.command_execution.destroy(cluster_id=cluster_id, context_id=context.id)
