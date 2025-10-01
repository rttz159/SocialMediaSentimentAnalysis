from pyspark.sql import SparkSession

# Author: LEWIS LIM RONG ZHEN

class DataLoader:
    def __init__(self, paths, row_cap=None):
        self.paths = paths
        self.row_cap = row_cap

    def create_session(self):
        return SparkSession.builder.appName("Task4_Neo4j_Ingest").getOrCreate()

    def _load_df(self, spark, path):
        df = spark.read.parquet(path)
        if self.row_cap:
            df = df.limit(self.row_cap)
        return df

    def load_all(self):
        spark = SparkSession.getActiveSession()
        return {
            "reddit_comment": self._load_df(spark, self.paths["reddit_comment"]),
            "reddit_post":    self._load_df(spark, self.paths["reddit_post"]),
            "lowyat_comment": self._load_df(spark, self.paths["lowyat_comment"]),
            "lowyat_post":    self._load_df(spark, self.paths["lowyat_post"]),
            "top_market":     self._load_df(spark, self.paths["top_market"]),
            "market_data":    self._load_df(spark, self.paths["market_data"]),
        }

    def print_overview(self, dfs):
        print("Loaded dataframes:")
        for name, df in dfs.items():
            print(name, "→", df.count(), "rows")
            df.printSchema()
