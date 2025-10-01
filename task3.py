from mongo.mongodb_ddl import MongoDDL
from mongo.mongodb_dml_company import CompanyDML
from mongo.mongodb_dml_market import MarketDML
from mongo.mongodb_dml_sentiment import SentimentDML

# Author: Tan Zi Yang

def main():
    MongoDDL().run()
    CompanyDML().run()
    MarketDML().run()
    SentimentDML().run()

if __name__ == "__main__":
    main()