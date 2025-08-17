# Updated Kafka Producer script using the confluent-kafka library.

import os, json, time
from random import randint, choice
from confluent_kafka import Producer #type:ignore
import sys

# Get the producer ID from environment variable or default to "4"
producer_id = os.getenv("PRODUCER_ID", "4")
topic = "anomaly-events"

# Configure the Kafka producer using the provided working configuration
# The bootstrap.servers should point to your Kafka broker's address.
conf = {
    'bootstrap.servers': 'kafka:29092',
    'acks': 'all',  # Wait for all in-sync replicas to acknowledge the message
    'retries': 5,   # Retry sending the message up to 5 times
    'enable.idempotence': True # Prevent duplicate messages
}

# Create a Kafka Producer instance
try:
    producer = Producer(conf)
except Exception as e:
    print(f"Failed to create producer: {e}")
    # Exit the script if the producer cannot be created
    sys.exit(1)

def delivery_report(err, msg):
    """
    Callback function to report on message delivery.
    This is called once for each message produced.
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

try:
    print(f"Starting producer {producer_id}... sending bursts of events to topic '{topic}'")
    while True:
        for _ in range(3):  # Send messages in bursts of 3
            # Create a dictionary for the event
            event = {
                "producer_id": producer_id,
                "event_id": randint(1000, 9999),
                "anomaly_score": round(randint(0, 100)/100, 2),
                "status": choice(["normal", "anomaly"]),
                "timestamp": time.time()
            }
            
            # Convert the dictionary to a JSON string and then to bytes
            event_value = json.dumps(event).encode('utf-8')
            
            # Asynchronously produce a message. The callback function is registered
            # to handle the delivery report when the message is sent.
            producer.produce(
                topic,
                value=event_value,
                callback=delivery_report
            )
            
            # Poll for events from the producer, like delivery reports.
            # This is a non-blocking call.
            producer.poll(0)
            
            # Print the sent message to the console
            print(f"Producer {producer_id} sent: {event}")
        
        # Wait for a short duration before sending the next burst
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProducer stopped manually.")
finally:
    # Flush any remaining messages to the broker.
    # This is important to ensure all messages are delivered before the script exits.
    producer.flush()
    print("Producer closed gracefully.")
