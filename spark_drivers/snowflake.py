from pyspark.sql import SQLContext
import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL 

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class snowflake(sparkBase):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def write(self,data):
        
        config = self.opts["opts"]["conf"]
        # url = config['to']['url']
        user = config['to']['user']
        password = config['to']['password']
        database = config['to']['database']
        schema = config['to']['schema']
        warehouse = config['to']['warehouse']
        table = config['to']['table']
        role = config['to']['role']
        account = config['to']['account']

        engine = create_engine(URL(
            account = account,
            user = user,
            password = password,
            database = database,
            schema = schema,
            warehouse = warehouse,
            role=role,
        ))
        
        connection = engine.connect()
        df = data.toPandas()
        
        df.to_sql(table, con=engine,if_exists='append', index=False) #make sure index is False, Snowflake doesnt accept indexes
        
        connection.close()
        engine.dispose()

        return True


    def read(self):
        spark = self.spark \
                .getOrCreate()
        
        config = self.opts["opts"]["conf"]
        url = config['from']['url']
        user = config['from']['user']
        password = config['from']['password']
        database = config['from']['database']
        schema = config['from']['schema']
        warehouse = config['from']['warehouse']
        table = config['from']['table']
        role = config['from']['role']
        account = config['from']['account']


        conn = snowflake.connector.connect(
                        user= user,
                        password= password,
                        account=account,
                        warehouse= warehouse,
                        database = database
                )

        # SQL query
        sql_query = f"SELECT * FROM {database}.{schema}.{table}"

        # Execute the query
        cursor = conn.cursor(DictCursor)
        cursor.execute(sql_query)

        # Fetch all results into a list of tuples
        results = cursor.fetchall()

        # Close Snowflake connection
        conn.close()

        # Create a Spark DataFrame from the fetched results
        spark_df = spark.createDataFrame(results)

        return spark_df
    
    def diagnose(self, mode):
        error_log = self.opts["opts"]["job_data"]['error_log']

        error_log.append("sqlserver diagnose not implemented")
        return {"status": False, "error_log": "snowflake diagnose not implemented"}

    

