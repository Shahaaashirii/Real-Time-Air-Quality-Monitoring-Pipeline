from kafka import KafkaProducer 
import requests
import json
import time

# ---------------------------
# Config
# ---------------------------
API_KEY = "71b9f2e2-3950-4a51-b307-7f9cb7ff7751"
CITIES = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946}
}
KAFKA_TOPIC = "air_quality"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# ---------------------------
# Kafka Producer
# ---------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------------------
# Stream + Push Logic
# ---------------------------
def fetch_and_publish():
    for city, coords in CITIES.items():
        lat, lon = coords["lat"], coords["lon"]
        url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={API_KEY}"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                pollution = data.get("data", {}).get("current", {}).get("pollution", {})
                aqi = pollution.get("aqius")
                main_pollutant = pollution.get("mainus")
                ts = pollution.get("ts")

                if aqi is not None and ts and main_pollutant:
                    payload = {
                        "city": city,
                        "aqi": aqi,
                        "main_pollutant": main_pollutant,
                        "ts": ts
                    }
                    producer.send(KAFKA_TOPIC, value=payload)
                    print(f" Sent to Kafka: {payload}")
                else:
                    print(f" Skipping {city}: Missing data")
            else:
                print(f" API error for {city}: {response.status_code}")
        except Exception as e:
            print(f" Error fetching {city}: {e}")

# ---------------------------
# Loop Forever (hourly fetch)
# ---------------------------
if __name__ == "__main__":
    while True:
        print("Fetching and publishing AQI data...")
        fetch_and_publish()
        time.sleep(120)  # Fetch every 2 minute
