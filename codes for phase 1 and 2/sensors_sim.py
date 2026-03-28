import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    now = datetime.utcnow()
    hour = now.hour + (now.minute / 60)
    
    # 1. Add Patterns: Create a daily cycle using a sine wave
    # Pollution and temp are usually higher in the middle of the day (hour 12-14)
    daily_cycle = math.sin(math.pi * (hour / 24.0)) 
    
    base_pm25 = 10 + (20 * daily_cycle) # Peaks around 30, drops to 10
    base_temp = 15 + (10 * daily_cycle) # Peaks around 25, drops to 15
    
    # Add a little natural noise
    pm2_5 = base_pm25 + random.uniform(-2, 2)
    temp = base_temp + random.uniform(-1, 1)
    
    # 2. Inject Complex Anomalies randomly (e.g., 5% chance)
    is_anomaly = False
    if random.random() < 0.05:
        is_anomaly = True
        # Create a "stealth" anomaly: PM2.5 spikes, but stays under a naive threshold of 50
        pm2_5 = pm2_5 + 15 
        # Temp drops weirdly
        temp = temp - 5

    data = {
        "sensor_id": "CAP_001",
        "timestamp": now.isoformat(),
        "location": {"lat": 48.8566, "lon": 2.3522},
        "measurements": {
            "pm2_5": round(pm2_5, 2),
            "pm10": round(pm2_5 * 1.5, 2), # Usually correlated with pm2.5
            "no2": round(random.uniform(10, 40), 2),
            "o3": round(random.uniform(20, 80), 2),
            "temperature": round(temp, 2),
            "humidity": random.randint(40, 80)
        },
        "status": "active",
        "is_true_anomaly": is_anomaly # We log this just to test our ML later!
    }

    producer.send("raw_data", value=data)
    print("Sent:", data)
    time.sleep(2)

