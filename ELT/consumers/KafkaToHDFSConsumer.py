from ..utils.ConsumerBuilder import ConsumerBuilder
import threading
import time

# Author: Raymond Teng Toh Zi

class KafkaToHDFSConsumer:
    def __init__(self, topic, hdfs_path, spark, app_prefix="kafka_processor", batch_size=1000):
        self.topic = topic
        self.hdfs_path = hdfs_path
        self.app_prefix = app_prefix
        self.batch_size = batch_size
        self.records = []
        self.spark = spark

    def consume_and_write(self, idle_seconds=600):
        with ConsumerBuilder(self.topic) as c:
            print(f"[{self.topic}] Consuming messages... will exit after {idle_seconds} seconds of inactivity.")

            last_message_time = time.time()

            try:
                while True:
                    msg_pack = c.consumer.poll(timeout_ms=1000) 

                    if not msg_pack:
                        if time.time() - last_message_time > idle_seconds:
                            print(f"[{self.topic}] No messages for {idle_seconds} seconds. Stopping.")
                            break
                        continue

                    for tp, records in msg_pack.items():
                        for record in records:
                            self.records.append(record.value)
                            print(f"Received {record.value}")
                            last_message_time = time.time()

                            if len(self.records) >= self.batch_size:
                                self._write_to_hdfs()
                                print(f"[{self.topic}] Wrote {self.batch_size} records to {self.hdfs_path}")
                                self.records = []

            except KeyboardInterrupt:
                print(f"[{self.topic}] Stopping consumer...")

            finally:
                if self.records:
                    self._write_to_hdfs()
                    print(f"[{self.topic}] Saved remaining records to {self.hdfs_path}")

    def _write_to_hdfs(self):
        if not self.records:
            print(f"[{self.topic}] No records to write.")
            return
        df_spark = self.spark.createDataFrame(self.records)
        df_spark.write.mode("append").parquet(self.hdfs_path)
        print(f"[{self.topic}] Wrote {len(self.records)} records to {self.hdfs_path}")

    @staticmethod
    def run_multiple(consumers):
        threads = [
            threading.Thread(target=consumer.consume_and_write)
            for consumer in consumers
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
