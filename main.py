import yaml
import os
import multiprocessing
from pyspark.sql import SparkSession
import csv
import pandas as pd

class main:
    def get_source_data(self):
        with open('configs/settings.yaml') as setting_file:
            settings = yaml.safe_load(setting_file.read())
        self.source_data = {}
        if settings:
            self.source_data = {"settings": settings}
            sources_list = {}
            for sources in settings:
                self.files = []
                if sources['source'] in ['csv','xls','parquet'] :
                    for file in os.listdir('inputs'):
                        if sources['source'] in file:
                            self.files.append(file)
                    sources_list[sources['name']] = self.files
            

            self.source_data["sources_list"] = sources_list
        return self.source_data
    

    def read_data(self,connection_name,name):
        
        self.df = pd.read_csv(f'inputs/{name}')
        return self.df

        
            
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

            



if __name__ == '__main__':
    main_ob = main()
    main_ob.run()