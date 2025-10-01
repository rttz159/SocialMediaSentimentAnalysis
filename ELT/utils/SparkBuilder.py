from pyspark.sql import SparkSession

# Author: Raymond Teng Toh Zi

class SparkBuilder:
    def __init__(self, app_name="SparkApp", master="local[*]", configs=None):
        self.app_name = app_name
        self.master = master
        self.configs = configs or {}
        self.spark = None

    def __enter__(self):
        builder = SparkSession.builder.appName(self.app_name).master(self.master)
        
        for key, value in self.configs.items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        return self.spark  

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.spark:
            self.spark.stop()
