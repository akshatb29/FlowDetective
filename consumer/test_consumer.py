from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "anomaly-events",
    bootstrap_servers="localhost:29092",  
    auto_offset_reset="earliest",  
    enable_auto_commit=True,
    group_id="anomaly-detector",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Consumer started, listening to 'anomaly-events' topic...")

try:
    for message in consumer:
        event = message.value
        print(f"Received: {event}")

except KeyboardInterrupt:
    print("Consumer stopped manually")
finally:
    consumer.close()
