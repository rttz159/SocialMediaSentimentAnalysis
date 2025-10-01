# Author: Lee Chia Chia

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_parquet(self, path):
        return self.spark.read.parquet(path)
