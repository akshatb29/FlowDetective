from confluent_kafka import Producer #type:ignore
import json
import time
import random

# producer = Producer({'bootstrap.servers': 'localhost:29092',
#                        'security.protocol': 'PLAINTEXT'})
producer = Producer({
    'bootstrap.servers': 'localhost:29092',
    'acks': 'all',             # wait for leader + replicas
    'retries': 5,              # retry sending
    'enable.idempotence': True # prevent duplicates
})


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

anomaly_types = ["DDoS", "Malware", "Phishing", "Zero-Day Exploit", "Normal"]

print("Starting producer... sending events to 'anomaly-events' topic")

try:
    while True:
        event = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "src_ip": f"192.168.0.{random.randint(1, 255)}",
            "dst_ip": f"10.0.0.{random.randint(1, 255)}",
            "bytes_sent": random.randint(100, 10000),
            "anomaly_type": random.choice(anomaly_types)
        }
        
        producer.produce(
            'anomaly-events',
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        producer.poll(0)
        
        print(f"Sent: {event}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped manually")
finally:
    producer.flush()
    print("Producer closed gracefully")