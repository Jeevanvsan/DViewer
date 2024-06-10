from spark_drivers.spark_base import sparkBase
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession



class sqlserver(sparkBase):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.jdbc_jar_path = 'connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar' 

    def read(self):
        connection_name = self.opts["opts"]["connection_name"]
        settings = self.opts["opts"]["settings"][connection_name]

        user = settings["user"]
        password = settings["password"]
        table = self.opts["opts"]["name"]
        servername = settings["servername"]
        database = settings["database"]
        jdbc_url = f"jdbc:sqlserver://{servername};databaseName={database}"
        query = f"(SELECT * FROM {table}) AS alias"

        spark = self.spark\
            .config("spark.jars", self.jdbc_jar_path)\
            .getOrCreate()

        properties = {
            "encrypt":"true",
            "user": user,
            "password": password,
            "trustServerCertificate":"true",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        data = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
        return data
    
    

      
