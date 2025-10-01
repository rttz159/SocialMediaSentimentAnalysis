import threading
import time
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Author: Eyvon Chieng Chu Sing

HDFS_URL = "http://localhost:9870"
STOP_HDFS_PATH = "/user/student/temp/STOP"
hdfs_client = InsecureClient(HDFS_URL, user="student")


def stop_file_exists():
    return hdfs_client.status(STOP_HDFS_PATH, strict=False) is not None


class MarketDataStreamer:
    def __init__(self,
                 kafka_servers="localhost:9092",
                 kafka_topic="market_data_topic",
                 meta_path="hdfs://localhost:9000/user/student/processed_data/market/top_markets.parquet",
                 ohlc_output_path="hdfs://localhost:9000/user/student/task5/output/ohlc",
                 sector_output_path="hdfs://localhost:9000/user/student/task5/output/sector",
                 checkpoint_dir="hdfs://localhost:9000/user/student/task5/checkpoints"):

        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.meta_path = meta_path
        self.ohlc_output_path = ohlc_output_path
        self.sector_output_path = sector_output_path
        self.checkpoint_dir = checkpoint_dir

        self.spark = self._create_spark_session()
        self.market_data_schema = self._define_schema()
        self.meta_df = self._load_metadata()

    def _create_spark_session(self):
        spark = (
            SparkSession.builder
            .appName("MarketDataStreaming")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                    "io.delta:delta-spark_2.13:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _define_schema(self):
        return StructType([
            StructField("Date", StringType(), True),
            StructField("Stock Code", StringType(), True),
            StructField("Close", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Open", DoubleType(), True),
            StructField("Volume", DoubleType(), True)
        ])

    def _load_metadata(self):
        df = (
            self.spark.read.parquet(self.meta_path)
            .withColumnRenamed("Stock Code", "StockCode")
            .filter((col("StockCode").isNotNull()) & (col("StockCode") != "") & col("Sector").isNotNull())
            .dropDuplicates(["StockCode"])
        )
        return df

    def read_streaming_data(self):
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), self.market_data_schema).alias("data"))
            .select("data.*")
            .withColumnRenamed("Stock Code", "StockCode")
            .withColumnRenamed("Date", "MarketDate")
            .withColumn("MarketDate", to_timestamp(col("MarketDate"), "yyyy-MM-dd"))
            .filter(
                col("StockCode").isNotNull() &
                col("MarketDate").isNotNull() &
                col("Volume").isNotNull()
            )
            .withWatermark("MarketDate", "1 day")
        )
        return df

    def join_with_metadata(self, df):
        return df.join(self.meta_df, on="StockCode", how="left")

    def compute_ohlc(self, df):
        ohlc_df = (
            df.groupBy("StockCode", window(col("MarketDate"), "1 day"))
            .agg(
                avg("Open").alias("open"),
                avg("High").alias("high"),
                avg("Low").alias("low"),
                avg("Close").alias("close"),
                avg("Volume").alias("avg_volume")
            )
            .select(
                col("StockCode"),
                col("window.start").alias("date"),
                "open", "high", "low", "close", "avg_volume"
            )
        )
        return ohlc_df

    def compute_sector_trends(self, df):
        sector_df = (
            df.groupBy("Sector", window(col("MarketDate"), "1 day"))
            .agg(
                avg("Close").alias("avg_close"),
                avg("Volume").alias("avg_volume")
            )
            .select(
                col("Sector"),
                col("window.start").alias("date"),
                col("avg_close"),
                col("avg_volume")
            )
        )
        return sector_df

    def write_stream(self, df, output_path, checkpoint_subdir, mode="append"):
        return (
            df.writeStream
            .format("delta")
            .outputMode(mode)
            .option("checkpointLocation", f"{self.checkpoint_dir}/{checkpoint_subdir}")
            .option("path", output_path)
            .start()
        )

    def _stop_file_watcher(self):
        while True:
            if stop_file_exists():
                self.logger.info("Stop file detected on HDFS. Stopping streaming queries...")
                for query in self.spark.streams.active:
                    query_name = getattr(query, "name", str(query))
                    self.logger.info(f"Stopping streaming query: {query_name}")
                    query.stop()
                self.logger.info("All streaming queries stopped gracefully.")
                break
            time.sleep(1)

    def start(self):
        market_df = self.read_streaming_data()
        joined_df = self.join_with_metadata(market_df)

        ohlc_df = self.compute_ohlc(joined_df)
        sector_df = self.compute_sector_trends(joined_df)

        self.write_stream(ohlc_df, self.ohlc_output_path, "ohlc")
        self.write_stream(sector_df, self.sector_output_path, "sector")

        watcher_thread = threading.Thread(target=self._stop_file_watcher, daemon=True)
        watcher_thread.start()

        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    streamer = MarketDataStreamer()
    streamer.start()
