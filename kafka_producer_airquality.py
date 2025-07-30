import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "air_quality"

# Cities for mock data
cities = ["Delhi", "Mumbai", "Kolkata", "Chennai", "Bengaluru"]
pollutants = ["pm2.5", "pm10", "o3", "no2", "so2", "co"]

while True:
    for city in cities:
        mock_data = {
            "city": city,
            "aqi": random.randint(50, 300),  # realistic AQI values
            "main_pollutant": random.choice(pollutants),
            "ts": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')  # ISO 8601 timestamp
        }

        # Send to Kafka
        producer.send(topic, value=mock_data)
        producer.flush()
        print(f"Mock Producer: {mock_data}")
        
        time.sleep(1)  # 1 second delay per message

