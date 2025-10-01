import logging
import threading
import time
import signal
import sys
import redis
from ELT.scrapers.MarketDataScraper import MarketDataScraper

# Author: Eyvon Chieng Chu Sing

class MarketDataProducer:
    def __init__(
        self,
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        kafka_topic="market_data_topic",
        publish_delay=0.5,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.kafka_topic = kafka_topic
        self.publish_delay = publish_delay

        self.logger = logging.getLogger("market_data_producer")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
        self.logger.addHandler(handler)

        self.redis_conn = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, decode_responses=True)

        self.stop_event = threading.Event()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.thread = threading.Thread(target=self._run_scraper_thread, name="market-data-thread")
        self.thread.daemon = True

    def _signal_handler(self, signum, frame):
        self.logger.info("Received termination signal. Shutting down...")
        self.stop_event.set()

    def _run_scraper_thread(self, days: int = 180):
        self.logger.info("Starting MarketDataScraper job")

        try:
            scraper = MarketDataScraper(self.redis_conn)

            self.logger.info("Scraping market price history for %s days...", days)
            market_data = scraper.run(days=days)

            if getattr(market_data, "empty", False):
                self.logger.warning("MarketDataScraper returned no data")
                return

            self.logger.info("Scraped %d market rows", len(market_data))

            if hasattr(scraper, "publish_kafka"):
                self.logger.info(
                    "Publishing market rows to Kafka topic '%s' with delay=%s",
                    self.kafka_topic,
                    self.publish_delay,
                )
                scraper.publish_kafka(market_data, self.kafka_topic, delay=self.publish_delay)
                self.logger.info("Market data published to Kafka")
            else:
                self.logger.error("MarketDataScraper has no 'publish_kafka' method")

        except Exception as exc:
            self.logger.exception("Uncaught error in MarketDataScraper: %s", exc)

    def start(self):
        self.logger.info("Starting market data producer")
        self.thread.start()

        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.stop_event.set()

        self.logger.info("Waiting for thread to finish (grace period)...")
        self.thread.join(timeout=5)
        self.logger.info("Producer process exiting")


if __name__ == "__main__":
    producer = MarketDataProducer()
    producer.start()
