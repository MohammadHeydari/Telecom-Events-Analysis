from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='192.168.224.128:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_event():
    return {
        "user_id": random.randint(1, 1000),
        "timestamp": datetime.now().isoformat(),
        "data_mb": random.randint(1, 2000),
        "call_duration": random.randint(0, 1000),
        "operator": random.choice(["irancell", "mci", "rightel"])
    }

while True:
    event = generate_event()
    producer.send("telecom_events", event)
    print("sent:", event)
    time.sleep(0.5)