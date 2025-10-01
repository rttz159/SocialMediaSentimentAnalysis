from pyspark.sql import SparkSession
import os

# Author: Lee Chia Chia

class SparkSessionBuilder:
    @staticmethod
    def get_session(app_name="DataCleaning", python_env="de-venv/bin/python"):
        os.environ["PYSPARK_PYTHON"] = python_env
        return SparkSession.builder.appName(app_name).getOrCreate()
