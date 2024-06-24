import yaml
import os
import sys
import multiprocessing
from pyspark.sql import SparkSession
from utils import json_path
import csv
import pandas as pd
from pyspark.sql.functions import col, trim
from sqlfluff.core.parser import Lexer, Parser
from sqlfluff.core import FluffConfig
import snowflake.connector
from snowflake.connector import DictCursor
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL 
from threading import Thread
from queue  import Queue


from multiprocessing.pool import ThreadPool


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



class main:
    def __init__(self):
        jdbc_jar_path = 'connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar' 
        self.spark = SparkSession.builder.appName("DViewer").config("spark.executor.cores", '3').config("spark.jars", jdbc_jar_path).getOrCreate()
    
    def get_source_data(self):
        with open('configs/settings.yaml') as setting_file:
            settings = yaml.safe_load(setting_file)

        


        self.source_data = {}
        if settings:
            q = Queue()
            worker = 2
            self.sources_list = {}
            for name,sources in settings.items():
                q.put(sources)
                # self.source_read(sources,settings) 
            for i in range(worker):
                t = Thread(target=self.run_task, args=(self.source_read,settings,q))
                t.daemon = True
                t.start()

            q.join()
                  
            self.source_data["sources_list"] = self.sources_list
        return self.source_data
    
    def run_task(self, fn,settings,q):
        while not q.empty():
            value = q.get()
            fn(value,settings)
            q.task_done()

    
    def get_column_data(self,file,source):
        if source in ["csv", "parquet"]:
            data = self.spark.read.format(source).load(f"inputs/{file}",header=True)
            struct = [
                    {"name": field.name,
                    "dataType": field.dataType.simpleString()
                    }
                    for field in data.schema.fields
                ]
            
            return struct

            

    
    def source_read(self,sources,settings):
        self.files = []
        if sources['source'] in ['csv','xlsx','parquet'] :
            self.source_data = {"settings": settings}

            for file in os.listdir('inputs'):
                if sources['source'] in file:
                    file_data = {}
                    file_data["name"] = file
                    file_data["columns"] =  self.get_column_data(file,sources['source'])

                    self.files.append(file_data)
            
            self.sources_list[sources['name']] = self.files

        elif sources['source'] in 'sqlserver' :

            server = sources['server']
            database = sources['database']
            username = sources['user']
            password = sources['password']
            schema = sources['schema']
            self.source_data = {"settings": settings}

            jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"

            # Define the query to fetch table names
            query = f"(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema}') AS table_list"
            properties = {
                            "encrypt":"true",
                            "user": username,
                            "password": password,
                            "trustServerCertificate":"true",
                            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        }
            data = self.spark.read.jdbc(url=jdbc_url, table=query, properties=properties)


            


            ##print("on sql settings reading done")

            source_data = {}


            # table_names = [row.TABLE_NAME for row in data.collect()]
            table_names = []

            for row in data.collect():
                table_data = {}
                table_data['name'] = row.TABLE_NAME

                query1 = f"(SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{row.TABLE_NAME}') AS table_list"

                column_data = self.spark.read.jdbc(url=jdbc_url, table=query1, properties=properties)

                # Collect the data from the DataFrame
                column_data_collected = column_data.collect()

                # Transform the collected data into a list of dictionaries
                table_data['columns'] = [{"name": row["column_name"], "dataType": row["data_type"]} for row in column_data_collected]

                table_names.append(table_data)



            # for tbls in table_names:
            #     source_data[''] = 

            #     query1 = f"(SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tbls}';) AS table_list"

            #     column_data = self.spark.read.jdbc(url=jdbc_url, table=query1, properties=properties)





            # ##print(table_names)

            sources['server'] = sources['server'].replace('\\', '\\\\')




            if 'table' in sources: 
                self.files.append(sources['table'])
            else:
                self.files = table_names
            
            self.sources_list[sources['name']] = self.files

        elif sources['source'] in 'snowflake' :

            account = sources['account']
            user= sources['user']
            password = sources['password']
            database= sources['database']
            schema= sources['schema']
            warehouse= sources['warehouse']
            role= sources['role']

            ##print("on snowflake settings")


            conn = snowflake.connector.connect(
                            user= user,
                            password= password,
                            account=account,
                            warehouse= warehouse,
                            database = database
                    )
            



            sql_query = f"SELECT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE';"

            cursor = conn.cursor(DictCursor)
            cursor.execute(sql_query)

            results = cursor.fetchall()

            conn.close()

            data = self.spark.createDataFrame(results)

            table_names = [row.TABLE_NAME for row in data.collect()]

            if 'table' in sources: 
                self.files.append(sources['table'])
            else:
                self.files = table_names
            self.sources_list[sources['name']] = self.files

        elif sources['source'] in 'api' :
            pass
            
    

    def read_data(self,connection_name,name,source,flag = True):

        if ('csv' in source):
            if not flag:
                name = f"{name}.{source}"

            data = self.spark.read.format("csv").load(f"inputs/{name}",header=True)
            data = data.na.fill('Null')
            if not os.path.exists(f'fi/{connection_name}'):
                os.mkdir(f'fi/{connection_name}')

            data.toPandas().to_parquet(f'fi/{connection_name}/{name}.parquet',index=False)

            struct = {
                    field.name: {
                        "dataType": field.dataType.simpleString(),
                        "nullable": field.nullable,
                    }
                    for field in data.schema.fields
                }

            struct = pd.DataFrame.from_dict(struct, orient='index')
            struct.reset_index(inplace=True)
            struct.rename(columns={'index': 'column_name'}, inplace=True)

            if flag:
                
                data = data.toPandas()


            return {"data": data,"struct":struct}
        
        if ('parquet' in source):
            if not flag:
                name = f"{name}.{source}"

            data = self.spark.read.format("parquet").load(f"inputs/{name}",header=True)
            data = data.na.fill('Null')

            if not os.path.exists(f'fi/{connection_name}'):
                os.mkdir(f'fi/{connection_name}')

            data.toPandas().to_parquet(f'fi/{connection_name}/{name}.parquet',index=False)

            struct = {
                    field.name: {
                        "dataType": field.dataType.simpleString(),
                        "nullable": field.nullable,
                    }
                    for field in data.schema.fields
                }

            struct = pd.DataFrame.from_dict(struct, orient='index')
            struct.reset_index(inplace=True)
            struct.rename(columns={'index': 'column_name'}, inplace=True)
            
            if flag:
                

                data = data.toPandas()


            return {"data": data,"struct":struct}
        
        if ('xlsx' in source):
            if not flag:
                name = f"{name}.{source}"

            df_pandas = pd.read_excel(f"inputs/{name}")  


            data = self.spark.createDataFrame(df_pandas)
            data = data.na.fill('Null')


            if not os.path.exists(f'fi/{connection_name}'):
                os.mkdir(f'fi/{connection_name}')

            data.toPandas().to_parquet(f'fi/{connection_name}/{name}.parquet',index=False)

            struct = {
                    field.name: {
                        "dataType": field.dataType.simpleString(),
                        "nullable": field.nullable,
                    }
                    for field in data.schema.fields
                }

            struct = pd.DataFrame.from_dict(struct, orient='index')
            struct.reset_index(inplace=True)
            struct.rename(columns={'index': 'column_name'}, inplace=True)
            
            if flag:

                

                data = data.toPandas()


            return {"data": data,"struct":struct}
        
        if('sqlserver' in source):
            
            with open('configs/settings.yaml') as setting_file:
                settings = yaml.safe_load(setting_file.read())

            
            connections = settings[connection_name]
            schema = connections['schema']

            user = connections['user']
            password = connections['password']
            table = f"{schema}.{name}"
            server = connections["server"]
            database = connections["database"]
            jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"



            query = f"(SELECT * FROM {table}) AS alias"


            properties = {
                "encrypt":"true",
                "user": user,
                "password": password,
                "trustServerCertificate":"true",
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            }
            data = self.spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
            data = data.na.fill('Null')

            query1 = f"(SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{name}') AS alias"
            struct = self.spark.read.jdbc(url=jdbc_url, table=query1, properties=properties)
            struct = struct.toPandas()


            if not os.path.exists(f'fi/{connection_name}'):
                            os.mkdir(f'fi/{connection_name}')

            data.toPandas().to_parquet(f'fi/{connection_name}/{name}.parquet',index=False)            
            if flag:
                data = data.toPandas()
                

            return {"data": data,"struct":struct}
        
        if('snowflake' in source):
            with open('configs/settings.yaml') as setting_file:
                settings = yaml.safe_load(setting_file.read())

            ##print('reading .....')
            connections = settings[connection_name]
            account = connections['account']
            user= connections['user']
            password = connections['password']
            database= connections['database']
            schema= connections['schema']
            warehouse= connections['warehouse']
            role= connections['role']


            conn = snowflake.connector.connect(
                user= user,
                password= password,
                account=account,
                warehouse= warehouse,
                database = database
            )



            # SQL query
            sql_query = f"SELECT * FROM {database}.{schema}.{name}"

            # Execute the query
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql_query)

            # Fetch all results into a list of tuples
            results = cursor.fetchall()

            sql_query = f"SELECT * FROM INFORMATION_SCHEMA.columns WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"

            # Execute the query
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql_query)

            struct = cursor.fetchall()


            # Close Snowflake connection
            conn.close()

            # Create a Spark DataFrame from the fetched results
            data = self.spark.createDataFrame(results)
            data = data.na.fill('Null')

            query1 = f"(SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{name}') AS alias"
            struct = self.spark.read.jdbc(url=jdbc_url, table=query1, properties=properties)
            struct = struct.toPandas()

            if not os.path.exists(f'fi/{connection_name}'):
                os.mkdir(f'fi/{connection_name}')

            data.toPandas().to_parquet(f'fi/{connection_name}/{name}.parquet',index=False)
            if flag:
                data = data.toPandas()
                

            return {"data": data,"struct":struct}
        

        

    def apply_filters(self,filters,fid,name,connection_name):

        filtered_df = self.spark.read.format("parquet").load(f"fi/{connection_name}/{name}.parquet")

        for id,filter in filters[fid].items():
            #print(filter)
            
            if ('equal' == filter['logic1_op']):
            
                filtered_df = self.equal_filter(filtered_df,filter['logic1_val'],filter['column_name'])

            if ('not equal' == filter['logic1_op']):
                filtered_df = self.not_equal_filter(filtered_df,filter['logic1_val'],filter['column_name'])

            if ('contains' == filter['logic1_op']):
                filtered_df = self.contains_filter(filtered_df,filter['logic1_val'],filter['column_name'])
            
            if ('not contains' == filter['logic1_op']):
                filtered_df = self.not_contains_filter(filtered_df,filter['logic1_val'],filter['column_name'])
            
            if ('starts with' == filter['logic1_op']):
                filtered_df = self.startsWith_filter(filtered_df,filter['logic1_val'],filter['column_name'])
            
            if ('ends with' == filter['logic1_op']):
                filtered_df = self.endsWith_filter(filtered_df,filter['logic1_val'],filter['column_name'])

            if ('null' == filter['logic1_op']):
                filtered_df = self.null_filter(filtered_df,filter['column_name'])

            if ('not null' == filter['logic1_op']):
                filtered_df = self.not_null_filter(filtered_df,filter['column_name'])
        # ##print(filtered_df.show())
        filtered_df = filtered_df.toPandas()

        return filtered_df
        
    def equal_filter(self,df,logic1_val,column_name):
        return df.filter(trim(trim(col(column_name))) == str(logic1_val).strip())

    def not_equal_filter(self,df,logic1_val,column_name):
        return df.filter(trim(col(column_name)) != str(logic1_val).strip())


    def contains_filter(self,df,logic1_val,column_name):
        return df.filter(trim(col(column_name)).like(f"%{str(logic1_val).strip()}%"))

    def not_contains_filter(self,df,logic1_val,column_name):
        return df.filter(~trim(col(column_name)).like(f"%{str(logic1_val).strip()}%"))

    def startsWith_filter(self,df,logic1_val,column_name):
        return df.filter(trim(col(column_name)).like(f"{str(logic1_val).strip()}%"))

    def endsWith_filter(self,df,logic1_val,column_name):
        return df.filter(trim(col(column_name)).like(f"%{str(logic1_val).strip()}"))
    
    def null_filter(self,df,column_name):
        return df.filter(trim(trim(col(column_name))) == str('Null').strip())
    
    def not_null_filter(self,df,column_name):
        return df.filter(trim(trim(col(column_name))) != str('Null').strip())
    

    def query_sheet(self,data):

        global df 
        # global filtered_df 

        try:

            with open('configs/settings.yaml') as setting_file:
                settings = yaml.safe_load(setting_file)

            query = str(data['query'])
            connection = data['connection']
            table = data['table']

            # ##print(connection)


            source = settings[connection]['source']

            if connection in os.listdir('fi'):
                if table in os.listdir(f"fi/{connection}"):
                    if source in ['csv','xls','parquet']:
                        table = f"{table}.{source}"
                    df = self.spark.read.format("parquet").load(f"fi/{connection}/{table}.parquet")
                else:
                    df = self.read_data(connection,table,source,False)
                    df = df['data']


                # df.show()
            else:
                df = self.read_data(connection,table,source,False)
                df = df['data']

            # df.show()

            df.createOrReplaceTempView(str(table))

            checked_query = self.check_query(query)

            if checked_query:
                filtered_df = self.spark.sql(query)
            # filtered_df = self.spark.sql(query)
                return filtered_df.toPandas()
            else:
                return {'error':  "provide only DQL queries"}

            # filtered_df.show()
        except Exception as e:
            return {'error':  str(e)}


    def check_query(self, query):
        config = FluffConfig(overrides=dict(dialect='tsql'))
        tokens, _ = Lexer(config=config).lex(query)
        tree = Parser(config=config).parse(tokens)
        records = tree.as_record(code_only=False, show_raw=True)

        parsed = dict([*list(records.items())])

        dql_keywords = ['SELECT', 'DESCRIBE', 'SHOW', 'EXPLAIN', 'WITH_COMPOUND']

        # ##print(parsed)

        stmts = json_path.rtn_get_json_keypaths(parsed,'file.batch.statement', top_level=True)[0]
        for key, value in stmts.items():
            ##print(key)
            if str(key.replace('_statement','')).upper() in dql_keywords:
                return True
            else:
                return False


            
        



if __name__ == '__main__':
    main_ob = main()
    main_ob.run()