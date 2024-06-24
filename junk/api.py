import os
import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Data Processing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Fetch data from the API
url = "https://www.alphavantage.co/query"
headers = {
    "Accept": "application/json",
}
params = {
    "function": "TIME_SERIES_DAILY",
    "symbol": "IBM",
    "apikey": "demo"
}

response = requests.get(url, headers=headers,params=params)

data = response.json()
# print(data["Time Series (Daily)"])

# Convert JSON data to RDD
rdd = spark.sparkContext.parallelize([json.dumps(data["Time Series (Daily)"])])

# Read RDD as DataFrame


df = spark.read.json(rdd)


# Flatten the DataFrame function
def flatten_df(df):
    def flatten_struct(df, prefix=None):
        if prefix is None:
            prefix = []
        for field in df.schema.fields:
            column_name = '.'.join(prefix + [field.name])
            if isinstance(field.dataType, StructType):
                df = flatten_struct(df.withColumnRenamed(field.name, column_name), prefix + [field.name])
            else:
                df = df.withColumnRenamed(field.name, column_name)
        return df
    
    return flatten_struct(df)

flat_df = flatten_df(df)

# Show DataFrame
flat_df.show(truncate=False)
flat_df.printSchema()

