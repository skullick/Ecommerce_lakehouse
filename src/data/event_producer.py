import json
import logging
import datetime
from typing import Dict, Any
from kafka import KafkaProducer

class EventProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, cls=CustomJSONEncoder).encode('utf-8'),
                retries=3
            )
            logging.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            self.producer = None

    def send_event(self, topic: str, event: Dict[str, Any]):
        if not self.producer:
            logging.warning("Kafka producer not connected. Dropping event.")
            return

        try:
            # We don't block on send for high throughput, but we can log errors via callbacks if needed
            self.producer.send(topic, event)
        except Exception as e:
            logging.error(f"Failed to send event to Kafka: {e}")

    def flush(self):
        if self.producer:
            self.producer.flush()

import uuid

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)
