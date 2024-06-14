import os
import sys
from pyspark.sql import SparkSession
import pandas as pd
import yaml

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


jdbc_jar_path = 'connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar' 
spark = SparkSession.builder.appName("DViewer").config("spark.executor.cores", '3').config("spark.jars", jdbc_jar_path).getOrCreate()
    
# with open('configs/settings.yaml') as setting_file:
#     settings = yaml.safe_load(setting_file.read())

# connection_name = 'sqlserver_connection'
# connections = settings[connection_name]
# schema = connections['schema']
# name = 'Address'

# user = connections['user']
# password = connections['password']
# table = f"{schema}.{name}"
# server = connections["server"]
# database = connections["database"]
# jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"

# properties = {
#                 "encrypt":"true",
#                 "user": user,
#                 "password": password,
#                 "trustServerCertificate":"true",
#                 "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#             }


# query1 = f"(SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{name}') AS alias"
# struct = spark.read.jdbc(url=jdbc_url, table=query1, properties=properties)
# struct = struct.toPandas()

# print(struct)

# Create a sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Get the schema as a dictionary
# Get the schema as a dictionary with more details
schema_dict_detailed = [
    {"name": field.name,
    "dataType": field.dataType.simpleString()
    }
    for field in df.schema.fields
]
# for field in df.schema.fields:
#     print(field.name)


print(schema_dict_detailed)

# # Convert the dictionary to a Pandas DataFrame
# schema_df = pd.DataFrame.from_dict(schema_dict_detailed, orient='index')
# schema_df.reset_index(inplace=True)
# schema_df.rename(columns={'index': 'column_name'}, inplace=True)

# print(schema_df)