from colors import red, green, yellow
import os
import sys
import yaml
from spark_drivers.spark_base import sparkBase
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class csv(sparkBase):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def read(self):
        spark = self.spark.getOrCreate()
        filename = self.opts["opts"]['name']
        data = spark.read.format("csv").load(f"inputs/{filename}",header=True)
        return data
    
    
        
