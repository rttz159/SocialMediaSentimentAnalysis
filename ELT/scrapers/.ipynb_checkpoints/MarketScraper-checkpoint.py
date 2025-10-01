import pandas as pd
import time
import json
import requests
from ..utils.ProducerBuilder import ProducerBuilder

# Author: Raymond Teng Toh Zi

class MarketScraper:
    def __init__(self, r, topic, aliases):
        self.topic = topic
        self.redis = r
        self.manual_aliases = aliases

    def fetch_market_data(self, url):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/115.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # raise exception if HTTP error
    
        tables = pd.read_html(response.text)
        table = tables[2]  # keep your original table selection
    
        table["Stock Code"] = table["Stock Code"].astype(str).str.strip() + ".KL"
        table["Timestamp"] = int(time.time())
        table["Company"] = table["Constituent Name"].str.upper().str.strip()
        table["Stock Code"] = table["Stock Code"].str.replace(".KL", "")
        return table

    def update_redis(self, table):
        self.redis.delete("markets_cons:markets")
        self.redis.rpush("markets_cons:markets", *table["Stock Code"].tolist())

        existing_fields = self.redis.hkeys("markets_cons:company_keywords")
        if existing_fields:
            self.redis.hdel("markets_cons:company_keywords", *existing_fields)

        for _, row in table.iterrows():
            name = row["Company"]
            code = row["Stock Code"]
            base_keywords = [
                name.lower(),
                name.lower().replace("berhad", "").strip(),
                code,
                code.lower()
            ]
            if name in self.manual_aliases:
                base_keywords.extend(self.manual_aliases[name])
            keywords = sorted(set(k.strip() for k in base_keywords if k))
            self.redis.hset("markets_cons:company_keywords", name, json.dumps(keywords))

        print("Redis hash populated markets_cons:company_keywords")

    def publish_to_kafka(self, records):
        with ProducerBuilder() as p:
            for record in records:
                p.produce(self.topic, record)
            print("Kafka messages sent.")

    def run(self, url):
        table = self.fetch_market_data(url)
        print(f"Pushing {len(table)} stock records to Kafka and Redis.")
        self.update_redis(table)
        records = table.to_dict(orient="records")
        self.publish_to_kafka(records)
