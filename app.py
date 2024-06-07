from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.connect.session import DatabricksSession as SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pyspark.sql.functions as F


# Generic Imports
import json
import pandas as pd
import streamlit as st
import os

CLUSTER_ID = "0601-182128-dcbte59m"

# Establish Databricks connection
@st.cache_resource
def get_databricks_client():
    w = WorkspaceClient(
        profile="e2demo",
        cluster_id=CLUSTER_ID
    )
    
    return w

# Fetch data
@st.cache_data(ttl=600)
def fetch_data_as_pandas_df_using_dbconnect(query):
    client = get_databricks_client()
    client_config = client.config
    spark = SparkSession.builder.sdkConfig(client_config).getOrCreate()
    # context = client.command_execution.create(
    #     cluster_id=CLUSTER_ID,
    #     language=compute.Language.PYTHON
    # ).result()

    # text_results = client.command_execution.execute(
    #     cluster_id=CLUSTER_ID,
    #     context_id=context.id,
    #     language=compute.Language.PYTHON,
    #     command=query
    # ).result()
    
    # # Poll the query status until it is complete
    # while True:
    #     status = client.statement_execution.get_status(
    #         statement_id=query_result.statement_id
    #     ).status
        
    #     if status == QueryStatus.SUCCEEDED or status == QueryStatus.FAILED:
    #         break

    # if status == QueryStatus.FAILED:
    #     raise Exception("Query failed to execute")

    # # Fetch the result
    # result = client.statement_execution.get_result(
    #     statement_id=query_result.statement_id
    # )
    # data = pd.DataFrame(
    #     result.data, columns=[col.name for col in result.columns]
    # )
    df = spark.read.table(
        "samples.nyctaxi.trips"
    )
    df = df.withColumn(
        "pickup_zip",
        col("pickup_zip").cast(StringType())
    ).withColumn(
        "dropoff_zip",
        col("dropoff_zip").cast(StringType())
    ).withColumn(
        "avg_trip_price",
        col("fare_amount")/col("trip_distance")
    ).withColumn(
        "some_user_input",
        F.lit(query)
    )
    df = df.createOrReplaceTempView("df_view")
    # df_new = spark.sql(
    #     query+" LIMIT 100"
    # )

    new_df = spark.sql(
        "select * from df_view limit 100"
    )
    
    # df_new_pdf = df_new.toPandas()
    new_df_pdf = new_df.toPandas()
    # return df_new_pdf
    return new_df_pdf

# Streamlit app
st.title('Simple Databricks Streamlit App')

query = st.text_area("Enter a value of your new column here")

if st.button("Run Query"):
    if query.strip() != "":
        try:
            data = fetch_data_as_pandas_df_using_dbconnect(query)
            st.dataframe(data)
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid SQL query.")

st.write(
    "This app connects to a Databricks cluster, executes a query, and displays the results in a dataframe."
)
