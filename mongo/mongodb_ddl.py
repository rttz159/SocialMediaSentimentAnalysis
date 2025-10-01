from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid
from mongo.mongodb_utils import PyMongoUtils  

# Author: Tan Zi Yang

DB_NAME = "DE"   

class MongoDDL:
    def __init__(self, db_name: str = DB_NAME):
        self.db = PyMongoUtils().get_database(db_name)

        self.COMPANY   = "Company"
        self.SENTIMENT = "Sentiment_Daily"
        self.MARKET    = "Market_Daily"

    def _ensure_collection(self, name: str):
        if name in self.db.list_collection_names():
            return self.db[name]
        try:
            self.db.create_collection(name)
        except CollectionInvalid:
            pass
        return self.db[name]

    def ensure_collections(self):
        self._ensure_collection(self.COMPANY)
        self._ensure_collection(self.SENTIMENT)
        self._ensure_collection(self.MARKET)

    def ensure_indexes(self):
        company = self.db[self.COMPANY]
        company.create_index([("StockCode", ASCENDING)], unique=True, name="ux_stockcode")
        company.create_index([("Company", ASCENDING)], name="ix_company")

        sent = self.db[self.SENTIMENT]
        sent.create_index([("StockCode", ASCENDING), ("Date", ASCENDING)],
                          unique=True, name="ux_sent_sc_date")
        sent.create_index([("Date", ASCENDING)], name="ix_sent_date")

        market = self.db[self.MARKET]
        market.create_index([("StockCode", ASCENDING), ("Date", ASCENDING)],
                            unique=True, name="ux_mkt_sc_date")
        market.create_index([("Date", ASCENDING)], name="ix_mkt_date")

    def run(self):
        self.ensure_collections()
        self.ensure_indexes()
        print(f"DDL prepared on database '{self.db.name}'.")
        print(f"Collections: {self.COMPANY}, {self.SENTIMENT}, {self.MARKET}")

if __name__ == "__main__":
    ddl = MongoDDL()
    ddl.run()