from ..utils.ProducerBuilder import ProducerBuilder
import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta

# Author: Raymond Teng Toh Zi

class MarketDataScraper:
    def __init__(self, redis):
        self.redis = redis
        self.stock_codes = self.get_from_redis()

    def get_from_redis(self):
        return self.redis.lrange("markets_cons:markets", 0, -1)

    def run(self, days=30):
        print("Starting scrape from yFinance")
        all_data = []
    
        end_date = datetime.today()
        start_date = end_date - timedelta(days=days)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
    
        for stock_code in self.stock_codes:
            ticker = f"{stock_code}.KL"
            data = yf.download(ticker, start=start_str, end=end_str)
    
            if not data.empty:
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
    
                data = data.reset_index()
                data["Stock Code"] = stock_code
                data = data[["Date", "Stock Code", "Close", "High", "Low", "Open", "Volume"]]
                all_data.append(data)
    
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values(by=["Date", "Stock Code"]).reset_index(drop=True)
            return final_df
        else:
            return pd.DataFrame(columns=["Date", "Stock Code", "Close", "High", "Low", "Open", "Volume"])
    
        
    def publish_kafka(self, df, topic, delay=0.01):
        records = df.to_dict(orient='records')
        with ProducerBuilder() as p:
            for record in records:
                if isinstance(record["Date"], pd.Timestamp):
                    record["Date"] = record["Date"].strftime('%Y-%m-%d')
                p.produce(topic, record)
                time.sleep(delay)