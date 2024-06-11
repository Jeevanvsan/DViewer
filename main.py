import yaml
import os
import multiprocessing
from pyspark.sql import SparkSession
import csv
import pandas as pd
from pyspark.sql.functions import col, trim



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
                    sources['servername'] = sources['servername'].replace('\\', '\\\\')
                    self.source_data = {"settings": settings}

                    if 'table' in sources: 
                        self.files.append(sources['table'])
                    sources_list[sources['name']] = self.files
                    
            self.source_data["sources_list"] = sources_list
        return self.source_data
    

    def read_data(self,connection_name,name,source):
        

        if ('csv' in source):
            data = self.spark.read.format("csv").load(f"inputs/{name}",header=True)
            data.toPandas().to_parquet(f'fi/{name}.parquet',index=False)
            data = data.toPandas()
            return data
        
        if ('parquet' in source):
            data = self.spark.read.format("parquet").load(f"inputs/{name}",header=True)
            data.toPandas().to_parquet(f'fi/{name}.parquet',index=False)
            data = data.toPandas()
            return data
        
        if('sqlserver' in source):
            
            with open('configs/settings.yaml') as setting_file:
                settings = yaml.safe_load(setting_file.read())
            
            connections = settings[connection_name]
            user = connections['user']
            password = connections['password']
            table = name
            servername = connections["servername"]
            database = connections["database"]
            jdbc_url = f"jdbc:sqlserver://{servername};databaseName={database}"
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

            data = data.toPandas()
            return data

        
            
        # p1 = multiprocessing.Process(target=self.data_reading_process,args=(connection_name,name, )) 

        # # starting processes 
        # p1.start() 

        # # process IDs 
        # print("ID of process p1: {}".format(p1.pid)) 

        # # wait until processes are finished 
        # p1.join()

    # def data_reading_process(self,connection_name,name):
    #     spark = SparkSession.builder.appName("DViewer").getOrCreate()
    #     print("reading started")
    #     data = spark.read.format("csv").load(f"inputs/{name}",header=True)
    #     data.show()

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

            
# worksheet for
        # df.createOrReplaceTempView("my_table")
        # Query to filter out rows where 'column1' is not equal to 1
        # filtered_df = self.spark.sql(f"SELECT * FROM my_table WHERE {column_name} != {logic1_val}")
        # filtered_df = self.spark.sql(f"SELECT * FROM my_table WHERE ID != 2")
        # filtered_df.show()
        # print("hi")
        # return filtered_df


if __name__ == '__main__':
    main_ob = main()
    main_ob.run()