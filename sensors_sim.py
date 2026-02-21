import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": "CAP_001",
        "timestamp": datetime.utcnow().isoformat(),
        "location": {"lat": 48.8566, "lon": 2.3522},
        "measurements": {
            "pm2_5": round(random.uniform(5, 50), 2),
            "pm10": round(random.uniform(10, 80), 2),
            "no2": round(random.uniform(10, 100), 2),
            "o3": round(random.uniform(20, 120), 2),
            "temperature": round(random.uniform(15, 35), 2),
            "humidity": random.randint(30, 90)
        },
        "status": "active"
    }

    producer.send("raw_data", value=data)
    print("Sent:", data)

    time.sleep(2)  # send every 2 seconds

