from kafka import KafkaConsumer
import json

# Author: Raymond Teng Toh Zi

class ConsumerBuilder:
    def __init__(self, topics, bootstrap_servers='localhost:9092'):
        if isinstance(topics, str):
            self.topics = [topics]
        else:
            self.topics = topics

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def consume(self):
        print(f"Consuming from topic: {self.topic}")
        for message in self.consumer:
            yield message.value

    def close(self):
        if self.consumer:
            self.consumer.close()
