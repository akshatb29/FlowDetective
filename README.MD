🔐 Real-Time Anomaly Detection Pipeline (Kafka + ML + Elasticsearch + Kibana)

End-to-end streaming system that ingests network events, classifies attacks with ML in real time, and visualizes them in Kibana. Built to demonstrate scaling, load balancing, and fault tolerance.

✨ Highlights

4 Producers (slightly different traffic patterns) → Kafka topic (anomaly-events, 4 partitions)

3 Consumers (same logic, different IDs) in one consumer group → automatic partition balancing

ML inference with Random Forest (scikit-learn) → class prediction + probabilities

Elasticsearch indexing → Kibana Lens dashboards & live filtering

Health logging: registration, deregistration, heartbeats, failed-node detection

🗺️ High-Level Design (HLD)
flowchart LR
  subgraph Producers
    P1[Producer 1]
    P2[Producer 2]
    P3[Producer 3]
    P4[Producer 4]
  end

  subgraph Kafka[Kafka Broker (KRaft)]
    T[(Topic: anomaly-events\nPartitions: 4)]
    L[(Topic: service_logs)]
  end

  subgraph Consumers[Consumer Group: anomaly-detector]
    C1[Consumer 1\nID: consumer-1]
    C2[Consumer 2\nID: consumer-2]
    C3[Consumer 3\nID: consumer-3]
  end

  subgraph ML[ML Inference]
    RF[Random Forest\n(sklean)]
  end

  ES[(Elasticsearch Index: anomalies)]
  K[(Kibana Dashboards)]

  P1 --> T
  P2 --> T
  P3 --> T
  P4 --> T

  C1 --> T
  C2 --> T
  C3 --> T

  C1 --> RF --> ES
  C2 --> RF
  C3 --> RF

  C1 --> L
  C2 --> L
  C3 --> L

  ES --> K

📦 Tech Stack
Layer	Tooling	Notes
Stream Transport	Apache Kafka (single broker, KRaft)	Topic anomaly-events (4 partitions), service_logs for heartbeats
Producers	Python + kafka-python	4 variants: baseline / high-anomaly / extra-metadata / bursty
Consumers	Python + kafka-python, pandas, joblib	3 containers with unique IDs; same logic & group
ML	RandomForestClassifier (scikit-learn)	Outputs predicted_attack and class_probabilities
Storage	Elasticsearch	Index anomalies
Viz	Kibana	Lens dashboards (pie/line/metrics/tables)
🧾 Event Schema (Producer → Kafka)

Topic: anomaly-events
Content-Type: JSON (UTF-8)

{
  "timestamp": "2025-08-17 20:08:26",
  "src_ip": "192.168.0.42",
  "dst_ip": "10.0.0.105",
  "bytes_sent": 163,
  "meta": {
    "producer_id": "3",
    "burst": false,
    "note": "optional metadata per producer"
  }
}


Some producers enrich metadata (e.g., source_ip vs src_ip, burst markers). Consumers normalize fields before feature extraction.

Heartbeat / Service Log (Topic: service_logs)

{
  "node_id": "consumer-1",
  "timestamp": "2025-08-17T20:09:00.123456",
  "message_type": "HEARTBEAT", // or REGISTRATION / DEREGISTRATION
  "processed_count": 128
}

🧠 ML Model (Inference)

Algorithm: Random Forest (scikit-learn)

Target classes (example): ["Benign", "Phishing", "Malware", "Port Scan", "Zero-Day Exploit"]

Features (derived in consumer):

bytes_sent (numeric)

src_ip_int, dst_ip_int (IPv4 converted to 32-bit ints)

hour_of_day (0–23)

ip_diff = abs(src_ip_int - dst_ip_int)

Outputs written to ES:

{
  "timestamp": "2025-08-17 20:08:26",
  "src_ip": "192.168.0.42",
  "dst_ip": "10.0.0.105",
  "bytes_sent": 163,
  "predicted_attack": "Phishing",
  "class_probabilities": [0.005, 0.061, 0.136, 0.773, 0.025]
}


Model is loaded via joblib.load("rf_model.pkl"). Replace with your trained artifact as needed.

📈 Operational Metrics (what to track/demo)

Ingress rate: messages/sec from producers

Throughput: consumed msgs/sec per consumer

Consumer lag: per partition (proof of scaling)

Heartbeats: last seen per consumer (service_logs)

Anomaly counters: per class, per time bucket

Top talkers: src_ip by sum(bytes_sent)

🧪 Proof of Load Balancing (Interview Demo)

Start all consumers (same group_id, different IDs).

Check assignments:

docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group anomaly-detector \
  --describe


Kill consumer-2 → re-run the command → partitions reassign to consumer-1 & consumer-3.

Start a new consumer → watch rebalance again.

⚙️ Setup & Run
1) Prereqs

Docker & Docker Compose

(Optional) Conda env for local scripts

2) Environment (Conda)
conda create -n anomaly-detection python=3.9 -y
conda activate anomaly-detection
pip install -r requirements.txt

3) Bring up infra
docker-compose up -d

4) Create Kafka topic (4 partitions)
docker exec -it kafka kafka-topics --create \
  --topic anomaly-events \
  --bootstrap-server kafka:9092 \
  --partitions 4 --replication-factor 1

5) Run producers (examples)
# In separate terminals or as services in compose
python producer1.py
python producer2.py
python producer3.py
python producer4.py

6) Run consumers (same script logic, unique IDs)
python consumer1.py  # uses "consumer-1"
python consumer2.py  # uses "consumer-2"
python consumer3.py  # uses "consumer-3"

7) Kibana

Open http://localhost:5601

Create Data View (Index Pattern) → anomalies

Use Lens to build:

Pie: count by predicted_attack

Line: count over time (date histogram), split by predicted_attack

Bar: top src_ip by sum(bytes_sent)

Metric: total anomalies (NOT predicted_attack:"Benign")

🧹 Cleaning Local Data (before committing)

These folders grow fast and should not be in git:

rm -rf ./data ./logs ./elasticsearch-data ./kafka-data ./kibana-data


.gitignore should exclude them (and Conda/venv artifacts).

📁 Suggested Repo Layout
anomaly-pipeline/
├─ docker-compose.yml
├─ producers/
│  ├─ producer1.py
│  ├─ producer2.py
│  ├─ producer3.py
│  └─ producer4.py
├─ consumers/
│  ├─ consumer1.py
│  ├─ consumer2.py
│  └─ consumer3.py
├─ models/
│  └─ rf_model.pkl
├─ requirements.txt
├─ README.md
└─ .gitignore

🚀 Future Enhancements (nice to mention in interviews)

Multi-broker Kafka (RF=3) for true fault tolerance

Consumer autoscaling (KEDA/Kubernetes or Swarm)

Dead-letter topic for bad/poison messages

Schema Registry (Avro/Protobuf) for contracts

Alerting (Slack/email) on anomaly spikes or node failure

Prometheus + Grafana for infra metrics