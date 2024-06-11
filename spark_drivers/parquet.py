
from colors import red, green, yellow
import os

class parquet(sparkBase):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def read(self):
        spark = self.spark.getOrCreate()
        filename = self.opts["opts"]["conf"]["from"]["filename"]
        data = spark.read.format("parquet").load(f"Dataflo/input/{filename}")
        return data
    
    def write(self,data):
        if (os.path.exists("output") == False):
            os.mkdir("output")
        filename = self.opts["opts"]["conf"]["to"]["filename"]
        data.toPandas().to_parquet(f'Dataflo/output/{filename}',index=False)
        return True
    
    def diagnose(self,mode):

        error_log = self.opts["opts"]["job_data"]['error_log']


        cross = u'\u274c'
        tick = u'\u2714'
        filename = None
        spark = self.spark.getOrCreate()
        # Get the underlying SparkContext
        sc = spark.sparkContext

        # Set the log level to ERROR
        sc.setLogLevel("ERROR")
        print(mode)
        # spark.sparkContext.setLogLevel('ERROR')
        if "read" in mode:
            status = True
            try:
                filename = self.opts["opts"]["conf"]["from"]["filename"]
            except:
                status = False
                print(red("From filename was not found on configuration " + cross))
                error_log.append("From filename was not found on configuration ")
                return {"status": status,"error_log": error_log}
            
            if filename is not None:
                try:
                    data = spark.read.format("parquet").load(f"Dataflo/input/{filename}")
                except:
                    status = False
                    print(red(f"{filename} not found on input directory or error occured while reading file " + cross))
                    error_log.append(f"{filename} not found on input directory or error occured while reading file " )
                    return {"status": status,"error_log": error_log}

            if status:
                print(green("Reading diagnosis completed. No problems found " + tick))
                error_log.append("Reading diagnosis completed. No problems found  ")
                return {"status": status,"error_log": "Reading diagnosis completed. No problems found  "}
            
        elif "write" in mode:
            status = True
            try:
                filename = self.opts["opts"]["conf"]["to"]["filename"]
            except:
                status = False
                print(red("To filename was not found on configuration " + cross))
                error_log.append("To filename was not found on configuration " )
                return {"status": status,"error_log": error_log}
            # if filename is not None:
            #     try:
            #         data.toPandas().to_parquet(f'output/{filename}',index=False)
            #     except:
            #         status = False
            #         print(red(f"{filename} got error while writing " + cross))

            if status:
                print(green("Insertion diagnosis completed. No problems found " + tick))
                error_log.append("Insertion diagnosis completed. No problems found ")

                return {"status": status,"error_log": "Insertion diagnosis completed. No problems found "}


        

        
        
