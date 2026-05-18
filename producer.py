from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

VM_IP = "192.168.224.128"

producer = KafkaProducer(
    bootstrap_servers=f"192.168.224.128:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

operators = ["mci", "irancell", "rightel"]
event_types = ["call", "sms", "data"]

def generate_event():
    return {
        "msisdn": f"09{random.randint(100000000,999999999)}",
        "operator": random.choice(operators),
        "event_type": random.choice(event_types),
        "timestamp": datetime.utcnow().isoformat(),
        "cell_id": f"cell_{random.randint(1, 500)}",
        "bytes": random.randint(0, 5000),
        "duration": random.randint(0, 300)
    }

while True:
    event = generate_event()
    producer.send("telecom-events", event)
    print("sent:", event)
    time.sleep(0.2)