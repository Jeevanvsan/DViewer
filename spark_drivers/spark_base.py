from pyspark.sql import SparkSession

class sparkBase:
    def __init__(self,**opts):
        self.opts = opts
        source = self.opts["opts"]['source']

        if "sqlserver" in source:
            self.spark = SparkSession.builder \
                .appName("DataFlo") \
                .config("spark.jars", "connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar") 
        else:
            self.spark = SparkSession.builder \
                .appName("DataFlo") \
            
    def read(self):
        pass

