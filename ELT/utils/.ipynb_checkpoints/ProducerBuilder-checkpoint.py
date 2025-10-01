from kafka import KafkaProducer
import json

# Author: Raymond Teng Toh Zi

class ProducerBuilder:

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush()
        self.producer.close()

    def produce(self, topic, string):
        self.producer.send(topic, string)