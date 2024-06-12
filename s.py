from pyspark.sql import SparkSession

jdbc_jar_path = 'connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SQLServerTables") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

# Define SQL Server connection parameters
server = 'JEEVANV\SQLEXPRESS'  # Note the double backslash to escape the backslash
database = 'AdventureWorksLT2022'
username = 'jeevan.v'
password = 'experion@123'
schema = 'SalesLT'

jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"


# Define the query to fetch table names
query = f"(SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema}') AS table_list"
properties = {
                "encrypt":"true",
                "user": username,
                "password": password,
                "trustServerCertificate":"true",
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            }
data = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

data.show()
