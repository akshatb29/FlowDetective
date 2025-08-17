# Updated script using confluent_kafka

from confluent_kafka import Producer #type:ignore
import json
import time
import random
import sys

# Configure the Kafka producer
# The bootstrap.servers address must match your Kafka broker's address.
# The `acks`, `retries`, and `enable.idempotence` settings
# are good practices for reliable message delivery.
conf = {
    'bootstrap.servers': 'localhost:29092',
    'acks': 'all',
    'retries': 5,
    'enable.idempotence': True
}

# Create a Kafka Producer instance
try:
    producer = Producer(conf)
except Exception as e:
    print(f"Failed to create producer: {e}")
    sys.exit(1)

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery success or failure.
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

anomaly_types = ["DDoS", "Malware", "Phishing", "Zero-Day Exploit", "Normal"]

print("Starting producer... sending events to 'anomaly-events' topic")

try:
    while True:
        # Create a dictionary for the event
        event = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "src_ip": f"192.168.0.{random.randint(1, 255)}",
            "dst_ip": f"10.0.0.{random.randint(1, 255)}",
            "bytes_sent": random.randint(100, 10000),
            "anomaly_type": random.choice(anomaly_types)
        }
        
        # Convert the dictionary to a JSON string and then to bytes
        event_value = json.dumps(event).encode('utf-8')
        
        # Asynchronously produce a message with a delivery report callback
        producer.produce(
            'anomaly-events',
            value=event_value,
            callback=delivery_report
        )
        
        # Poll for any events (like delivery reports)
        producer.poll(0)
        
        print(f"Sent: {event}")
        time.sleep(2)

except KeyboardInterrupt:
    print("\nProducer stopped manually.")
finally:
    # Flush any remaining messages to the broker and close the producer
    producer.flush()
    print("Producer closed gracefully.")