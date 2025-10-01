from pyspark.sql import SparkSession
import os
from mongo.mongodb_utils import PyMongoUtils

# Author: Tan Zi Yang

DB_NAME = "DE"

class CompanyDML:
    def __init__(self, db_name: str = DB_NAME):
        self.db = PyMongoUtils().get_database(db_name)
        self.COL = "Company"

        os.environ["PYSPARK_PYTHON"] = "de-venv/bin/python"
        self.spark = SparkSession.builder.appName("CompanyDML").getOrCreate()

        self.top_market_path = "hdfs://localhost:9000/user/student/processed_data/market/top_markets.parquet"

    def load_companies(self):
        df = self.spark.read.parquet(self.top_market_path)

        companies_df = (df
            .selectExpr("`Stock Code` as StockCode", "Company", "Sector")
            .dropDuplicates(["StockCode", "Company"])
        )

        company_docs = [
            {
                "_id": row["StockCode"],  
                "StockCode": row["StockCode"],
                "Company": row["Company"],
                "Sector": row["Sector"]
            }
            for row in companies_df.collect()
        ]

        return company_docs

    def upsert_many(self, companies):
        for doc in companies:
            self.db[self.COL].update_one(
                {"_id": doc["_id"]},    
                {"$set": doc},
                upsert=True
            )
        print(f"Upserted {len(companies)} companies into '{self.COL}' collection.")

    def run(self):
        companies = self.load_companies()
        self.upsert_many(companies)

if __name__ == "__main__":
    dml = CompanyDML()
    dml.run()