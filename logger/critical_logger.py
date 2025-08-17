from kafka import KafkaConsumer
import json
from datetime import datetime
import time

# ----------------------
# Configurations
# ----------------------
KAFKA_BROKER = "localhost:29092"
HEARTBEAT_TOPIC = "service_logs"
HEARTBEAT_TIMEOUT = 15  # seconds
CONSUMER_GROUP = "critical-logger"

consumer = KafkaConsumer(
    HEARTBEAT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Heartbeat tracker
last_heartbeat = {}

# ----------------------
# ANSI Colors
# ----------------------
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BLUE = "\033[94m"
PINK = "\033[95m"
RESET = "\033[0m"

# ----------------------
# Functions
# ----------------------
def check_failed_nodes():
    now = datetime.now()
    failed_nodes = []

    for node_id, ts in last_heartbeat.items():
        last_time = datetime.fromisoformat(ts)
        if (now - last_time).total_seconds() > HEARTBEAT_TIMEOUT:
            failed_nodes.append(node_id)

    for node_id in failed_nodes:
        print(f"{RED}[ALERT] Node {node_id} is considered FAILED!{RESET}")
        del last_heartbeat[node_id]

# ----------------------
# Main loop
# ----------------------
try:
    print("Critical Logger started, monitoring heartbeats, registrations, and deregistrations...")

    for msg in consumer:
        log = msg.value
        node_id = log.get("node_id")
        msg_type = log.get("message_type")
        log_level = log.get("log_level", "")

        # REGISTRATION
        if msg_type == "REGISTRATION":
            print(f"{PINK}[REGISTRATION] Node {node_id} registered for {log.get('service_name')}{RESET}")

        # DEREGISTRATION
        elif msg_type == "DEREGISTRATION":
            print(f"{PINK}[DEREGISTRATION] Node {node_id} deregistered for {log.get('service_name')} with status {log.get('status')}{RESET}")

        # HEARTBEAT
        elif msg_type == "HEARTBEAT":
            last_heartbeat[node_id] = log.get("timestamp")
            print(f"{BLUE}[HEARTBEAT] Node {node_id} alive at {log.get('timestamp')}{RESET}")

        # WARN / ERROR logs
        elif log_level == "WARN":
            print(f"{YELLOW}[WARN] Node {node_id}: {log.get('message')}{RESET}")
        elif log_level == "ERROR":
            print(f"{RED}[ERROR] Node {node_id}: {log.get('message')}{RESET}")

        # Periodically check failed nodes
        check_failed_nodes()
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Critical Logger stopped manually.")
finally:
    consumer.close()
    print("Critical Logger closed.")
