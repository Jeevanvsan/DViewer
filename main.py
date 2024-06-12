import yaml
import os
import multiprocessing
from pyspark.sql import SparkSession
from utils import json_path
import csv
import pandas as pd
from pyspark.sql.functions import col, trim
from sqlfluff.core.parser import Lexer, Parser
from sqlfluff.core import FluffConfig



class main:
    def __init__(self):
        jdbc_jar_path = 'connectors/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre8.jar' 
        self.spark = SparkSession.builder.appName("DViewer").config("spark.jars", jdbc_jar_path).getOrCreate()
    
    def get_source_data(self):
        with open('configs/settings.yaml') as setting_file:
            settings = yaml.safe_load(setting_file)

        
        self.source_data = {}
        if settings:
            sources_list = {}
            for name,sources in settings.items():
                self.files = []
                if sources['source'] in ['csv','xls','parquet'] :
                    self.source_data = {"settings": settings}

                    for file in os.listdir('inputs'):
                        if sources['source'] in file:
                            self.files.append(file)
                    sources_list[sources['name']] = self.files

                elif sources['source'] in ['sqlserver','snowflake'] :

                    server = sources['server']
                    database = sources['database']
                    username = sources['user']
                    password = sources['password']
                    schema = sources['schema']
                    self.source_data = {"settings": settings}

                    jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"

                    print("on sql settings")


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

                    print("on sql settings reading done")


                    table_names = [row.TABLE_NAME for row in data.collect()]

                    # print(table_names)

                    sources['server'] = sources['server'].replace('\\', '\\\\')




                    if 'table' in sources: 
                        self.files.append(sources['table'])
                    else:
                        self.files = table_names
                    sources_list[sources['name']] = self.files
                    
            self.source_data["sources_list"] = sources_list
        return self.source_data
    

    def read_data(self,connection_name,name,source,flag = True):
      
        

        if ('csv' in source):
            if not flag:
                name = f"{name}.{source}"

            data = self.spark.read.format("csv").load(f"inputs/{name}",header=True)
            data.toPandas().to_parquet(f'fi/{name}.parquet',index=False)
            if flag:
                data = data.toPandas()
            return data
        
        if ('parquet' in source):
            if not flag:
                name = f"{name}.{source}"

            data = self.spark.read.format("parquet").load(f"inputs/{name}",header=True)
            data.toPandas().to_parquet(f'fi/{name}.parquet',index=False)
            
            if flag:
                data = data.toPandas()
            return data
        
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
            data.toPandas().to_parquet(f'fi/{name}.parquet',index=False)
            if flag:
                data = data.toPandas()
            return data

        

    def apply_filters(self,filters,fid,name):

        filtered_df = self.spark.read.format("parquet").load(f"fi/{name}.parquet")

        for id,filter in filters[fid].items():
            print(filter)
            
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
        # print(filtered_df.show())
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
    

    def query_sheet(self,data):

        global df 
        # global filtered_df 

        try:

            with open('configs/settings.yaml') as setting_file:
                settings = yaml.safe_load(setting_file)

            query = str(data['query'])
            connection = data['connection']
            table = data['table']

            # print(query)


            source = settings[connection]['source']

            if table in os.listdir('fi'):
                if source in ['csv','xls','parquet']:
                    table = f"{table}.{source}"
                df = self.spark.read.format("parquet").load(f"fi/{table}.parquet")
                # df.show()
            else:
                df = self.read_data(connection,table,source,False)

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

        # print(parsed)

        stmts = json_path.rtn_get_json_keypaths(parsed,'file.batch.statement', top_level=True)[0]
        for key, value in stmts.items():
            print(key)
            if str(key.replace('_statement','')).upper() in dql_keywords:
                return True
            else:
                return False

        # return any(str(key.replace('_statement','')).upper() in dql_keywords for key in stmts)

            
        






if __name__ == '__main__':
    main_ob = main()
    main_ob.run()