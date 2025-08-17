🚀 Real-Time Anomaly Detection Pipeline with Kafka, ML & ELK
📌 Overview

This project implements a real-time anomaly detection system for cybersecurity.
It integrates:

Kafka – Streaming platform for reliable event ingestion.

Producers – Simulated network traffic events (with anomalies).

Consumers – Three independent consumers to demonstrate Kafka load balancing.

ML Model – Classifies events into attack categories (Normal, Phishing, Malware, Zero-Day Exploit, etc.).

Elasticsearch – Stores classified events for querying.

Kibana – Visualizes anomalies with dashboards & charts.

👉 This mimics how real-world security monitoring pipelines are built for detecting intrusions and malicious activity.

⚡ Features

✅ Distributed Streaming – Multiple producers and consumers with partitioned topics.
✅ Load Balancing Proof – Kill one consumer, Kafka rebalances partitions to others.
✅ Reliable Delivery – Producer uses acks=all & idempotence to avoid data loss.
✅ ML Integration – Real-time classification of network traffic into attacks.
✅ Kibana Dashboards – Visualize anomaly trends, top source IPs, attack categories.
✅ Scalable Design – Easy to extend with more consumers/brokers.

🐳 Setup Instructions
1️⃣ Clone the repository
git clone https://github.com/<your-username>/anomaly-detection-pipeline.git
cd anomaly-detection-pipeline

2️⃣ Create a Conda environment
conda create -n anomaly-detection python=3.9 -y
conda activate anomaly-detection
pip install -r requirements.txt

3️⃣ Start Docker stack
docker-compose up -d


This starts:

Kafka broker (on localhost:29092)

Zookeeper

Elasticsearch (on localhost:9200)

Kibana (on localhost:5601)

4️⃣ Run Producer(s)
python producer.py

5️⃣ Run Consumers

You can run 1, 2, or 3 consumers:

python consumer1.py
python consumer2.py
python consumer3.py


👉 Proof of load balancing:

Kill one consumer → partitions are reassigned to others.

Start a new consumer → it auto-joins the group.

6️⃣ View in Kibana

Go to http://localhost:5601

Create an index pattern anomalies-*

Explore raw logs & build dashboards for:

Attack types distribution

Top malicious source IPs

Anomalies over time