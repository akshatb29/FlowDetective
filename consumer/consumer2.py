from kafka import KafkaConsumer, KafkaProducer
import json
import time
from datetime import datetime
import pandas as pd
import joblib
from elasticsearch import Elasticsearch
import sys
import uuid

# ----------------------
# Configurations
# ----------------------
# Assign a unique consumer ID, with a default for running without arguments.
consumer_id = sys.argv[1] if len(sys.argv) > 1 else "consumer-2"
KAFKA_BROKER = "localhost:29092"
ANOMALY_TOPIC = "anomaly-events"
HEARTBEAT_TOPIC = "service_logs"
HEARTBEAT_INTERVAL = 10  # seconds

# Elasticsearch
es = Elasticsearch("http://localhost:9200")
ES_INDEX = "anomalies"

# ----------------------
# ANSI colors
# ----------------------
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BLUE = "\033[94m"
PINK = "\033[95m"
RESET = "\033[0m"

# Load Random Forest model
# The model path should be accessible from the script's execution directory.
try:
    rf_model = joblib.load("rf_model.pkl")
    print("Random Forest model loaded successfully.")
except FileNotFoundError:
    print("Error: 'rf_model.pkl' not found. Please ensure the model file is in the correct directory.")
    sys.exit(1)

# Kafka Consumer
consumer = KafkaConsumer(
    ANOMALY_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='anomaly-detector',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer (for heartbeat)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Heartbeat tracker
last_heartbeat_time = time.time()
msg_count = 0

# ----------------------
# Helper functions
# ----------------------
# Utility function to convert IP to integer, consistent with consumer 1.
def ip_to_int(ip):
    try:
        parts = list(map(int, ip.split('.')))
        return parts[0]*(256**3) + parts[1]*(256**2) + parts[2]*256 + parts[3]
    except (ValueError, IndexError):
        print(f"Warning: Invalid IP address format received: {ip}")
        return 0

def send_heartbeat():
    hb_msg = {
        "node_id": consumer_id,
        "timestamp": datetime.now().isoformat(),
        "message_type": "HEARTBEAT",
        "processed_count": msg_count
    }
    producer.send(HEARTBEAT_TOPIC, hb_msg)

def send_registration():
    reg_msg = {
        "node_id": consumer_id,
        "timestamp": datetime.now().isoformat(),
        "message_type": "REGISTRATION",
        "service_name": "anomaly_consumer"
    }
    producer.send(HEARTBEAT_TOPIC, reg_msg)

def send_deregistration():
    dereg_msg = {
        "node_id": consumer_id,
        "timestamp": datetime.now().isoformat(),
        "message_type": "DEREGISTRATION",
        "service_name": "anomaly_consumer",
        "status": "stopped"
    }
    producer.send(HEARTBEAT_TOPIC, dereg_msg)

# ----------------------
# Main loop
# ----------------------
try:
    send_registration()
    print(f"{GREEN}{consumer_id} started and registered.{RESET}")

    for msg in consumer:
        message = msg.value
        msg_count += 1

        # --- Feature extraction ---
        try:
            bytes_sent = message['bytes_sent']
            src_ip_int = ip_to_int(message['src_ip'])
            dst_ip_int = ip_to_int(message['dst_ip'])
            hour_of_day = int(message['timestamp'].split(' ')[1].split(':')[0])
            ip_diff = abs(src_ip_int - dst_ip_int)
        except KeyError as e:
            print(f"Skipping message due to missing key: {e} in message {message}")
            continue

        features_df = pd.DataFrame(
            [[bytes_sent, src_ip_int, dst_ip_int, hour_of_day, ip_diff]],
            columns=['bytes_sent', 'src_ip_int', 'dst_ip_int', 'hour_of_day', 'ip_diff']
        )

        # --- Prediction ---
        predicted_attack = rf_model.predict(features_df)[0]
        predicted_proba = rf_model.predict_proba(features_df)[0]

        print(f"Message: {message}")
        print(f"Predicted Attack: {predicted_attack}")
        print(f"Class Probabilities: {predicted_proba}")

        # --- Elasticsearch indexing ---
        doc = {
            "timestamp": message['timestamp'],
            "src_ip": message['src_ip'],
            "dst_ip": message['dst_ip'],
            "bytes_sent": message['bytes_sent'],
            "predicted_attack": predicted_attack,
            "class_probabilities": predicted_proba.tolist()
        }
        es.index(index=ES_INDEX, document=doc)

        # --- Heartbeat ---
        if time.time() - last_heartbeat_time >= HEARTBEAT_INTERVAL:
            send_heartbeat()
            last_heartbeat_time = time.time()
            print(f"[HEARTBEAT] {consumer_id} sent at {datetime.now().isoformat()}")

except KeyboardInterrupt:
    print(f"{YELLOW}{consumer_id} stopped manually.{RESET}")
finally:
    send_deregistration()
    consumer.close()
    producer.close()
    print(f"{PINK}{consumer_id} deregistered and closed.{RESET}")


