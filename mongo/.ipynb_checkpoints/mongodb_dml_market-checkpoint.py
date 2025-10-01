from pyspark.sql import SparkSession, functions as F
import os
from datetime import datetime
from mongo.mongodb_utils import PyMongoUtils

# Author: Tan Zi Yang

DB_NAME = "DE"

class MarketDML:
    def __init__(self, db_name: str = DB_NAME):
        self.db = PyMongoUtils().get_database(db_name)
        self.COL = "Market_Daily"

        os.environ["PYSPARK_PYTHON"] = "de-venv/bin/python"
        self.spark = SparkSession.builder.appName("MarketDML").getOrCreate()

        self.market_data_path = "hdfs://localhost:9000/user/student/processed_data/market_data/market_data.parquet"

    def load_market(self):
        df = self.spark.read.parquet(self.market_data_path)

        market_df = (df
            .withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))
            .withColumn("StockCode", F.col("Stock Code").cast("string"))
            .withColumn("Change", F.col("Close") - F.col("Open"))
            .select("StockCode", "Date", "Open", "High", "Low", "Close", "Volume", "Change")
        )

        market_docs = []
        for row in market_df.collect():
            market_docs.append({
                "_id": f"{row['StockCode']}_{row['Date'].strftime('%Y-%m-%d')}",
                "StockCode": row["StockCode"],
                "Date": datetime.strptime(str(row["Date"]), "%Y-%m-%d"), 
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Volume": int(row["Volume"]),
                "Change": row["Change"]
            })

        return market_docs

    def upsert_many(self, market_data):
        for doc in market_data:
            self.db[self.COL].update_one(
                {"_id": doc["_id"]},  
                {"$set": doc},
                upsert=True
            )
        print(f"Upserted {len(market_data)} records into '{self.COL}' collection.")

    def run(self):
        market_data = self.load_market()
        self.upsert_many(market_data)

if __name__ == "__main__":
    dml = MarketDML()
    dml.run()
