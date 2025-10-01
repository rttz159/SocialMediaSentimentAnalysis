from ELT.scrapers.MarketScraper import MarketScraper
from ELT.scrapers.MarketDataScraper import MarketDataScraper
from ELT.scrapers.RedditScraperManager import RedditScraperManager
from ELT.scrapers.LowyatScraper import LowyatScraper
from ELT.utils.CompanyMatcher import CompanyMatcher
from ELT.consumers.KafkaToHDFSConsumer import KafkaToHDFSConsumer
from ELT.utils.SparkBuilder import SparkBuilder
import os
import json
import redis
import threading
from dotenv import load_dotenv

# Author: Raymond Teng Toh Zi

class Task1:
    def __init__(self):
        load_dotenv()
        self.accounts = json.loads(os.getenv("ACCOUNTS_JSON"))

        self.keyword_aliases = {
            "99 SPEED MART RETAIL HOLDINGS": ["99speedmart", "99 mart", "99speed", "speedmart"],
            "AXIATA GROUP": ["axiata"],
            "CELCOMDIGI": ["celcomdigi", "celcom", "digi"],
            "CIMB GROUP HOLDINGS": ["cimb"],
            "GAMUDA": ["gamuda"],
            "HONG LEONG BANK": ["hlbank", "hlb"],
            "HONG LEONG FINANCIAL GROUP": ["hlfg"],
            "IHH HEALTHCARE": ["ihh", "ihhhealthcare"],
            "IOI CORPORATION": ["ioi", "ioicorp"],
            "KUALA LUMPUR KEPONG": ["klk"],
            "MALAYAN BANKING": ["maybank", "mbb", "maybk"],
            "MAXIS": ["maxis"],
            "MISC": ["misc", "miscbhd"],
            "MR D.I.Y. GROUP (M)": ["mrdiy", "mr.diy"],
            "NESTLE (M)": ["nestle", "nestlemy", "nestle malaysia"],
            "PETRONAS CHEMICALS GROUP": ["petchem", "pcg"],
            "PETRONAS DAGANGAN": ["petdag"],
            "PETRONAS GAS": ["petgas"],
            "PPB GROUP": ["ppb"],
            "PRESS METAL ALUMINIUM HOLDINGS": ["pressmetal", "pmetal", "press metal"],
            "PUBLIC BANK": ["pbb", "publicbank"],
            "QL RESOURCES": ["ql", "qlresources"],
            "RHB BANK": ["rhb"],
            "SIME DARBY": ["sime", "sime darby"],
            "SD GUTHRIE": ["sdg", "sdguthrie", "guthrie"],
            "SUNWAY": ["sunway"],
            "TELEKOM MALAYSIA": ["tm", "telekom", "telekom malaysia"],
            "TENAGA NASIONAL": ["tnb"],
            "YTL CORPORATION": ["ytl"],
            "YTL POWER INTERNATIONAL": ["ytlp", "ytlpowr", "ytl power"],
        }

        self.SUBREDDITS = [
            "MalaysianPF", "Malaysia", "investing", "Bursa_Malaysia",
            "malaysians", "malaysia", "Bolehland", "malaysiaFIRE"
        ]
        self.REDDIT_APPS = [self.accounts[f"acc{i}"] for i in range(1, 9)]

        self.redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        self.matcher = CompanyMatcher()

        self.market_hdfs_path = r"hdfs://localhost:9000/user/student/raw_data/market/top_markets.parquet"
        self.market_data_hdfs_path = r"hdfs://localhost:9000/user/student/raw_data/market_data/market_data.parquet"
        self.reddit_post_path = r"hdfs://localhost:9000/user/student/raw_data/reddit/post.parquet"
        self.reddit_comment_path = r"hdfs://localhost:9000/user/student/raw_data/reddit/comment.parquet"
        self.lowyat_post_path = r"hdfs://localhost:9000/user/student/raw_data/lowyat/post.parquet"
        self.lowyat_comment_path = r"hdfs://localhost:9000/user/student/raw_data/lowyat/comment.parquet"

    def run_scrapers(self):
        MarketScraper(self.redis_client, topic="top_markets", aliases=self.keyword_aliases) \
            .run("https://en.wikipedia.org/wiki/FTSE_Bursa_Malaysia_KLCI")

        reddit_manager = RedditScraperManager(
            reddit_accounts=self.REDDIT_APPS,
            subreddits=self.SUBREDDITS,
            matcher=self.matcher,
            r=self.redis_client,
            post_topic="reddit_posts",
            comment_topic="reddit_comments"
        )
        reddit_manager.run()
        
        market_data_scraper = MarketDataScraper(self.redis_client)
        df = market_data_scraper.run()
        market_data_scraper.publish_kafka(df, "yfinance_data")

        lowyat_scraper = LowyatScraper(self.matcher)
        lowyat_posts = lowyat_scraper.scrape_and_publish("lowyat_posts","lowyat_comments")
        lowyat_scraper.close()

    def run_consumers(self):
        with SparkBuilder(app_name="KafkaToHDFSAllTopics") as spark:
            consumers = [
                KafkaToHDFSConsumer("top_markets", self.market_hdfs_path, spark, app_prefix="top_markets_processor"),
                KafkaToHDFSConsumer("yfinance_data", self.market_data_hdfs_path, spark, app_prefix="yfinance_processor"),
                KafkaToHDFSConsumer("reddit_posts", self.reddit_post_path, spark, app_prefix="reddit_processor"),
                KafkaToHDFSConsumer("reddit_comments", self.reddit_comment_path, spark, app_prefix="reddit_processor"),
                KafkaToHDFSConsumer("lowyat_posts", self.lowyat_post_path, spark, app_prefix="lowyat_processor"),
                KafkaToHDFSConsumer("lowyat_comments", self.lowyat_comment_path, spark, app_prefix="lowyat_processor"),
            ]
            KafkaToHDFSConsumer.run_multiple(consumers)

    def run_pipeline(self):
        print("=== Starting Consumers ===")
        consumer_thread = threading.Thread(target=self.run_consumers)
        consumer_thread.start()
    
        print("=== Running Scrapers ===")
        self.run_scrapers()
    
        consumer_thread.join()


if __name__ == "__main__":
    Task1().run_pipeline()