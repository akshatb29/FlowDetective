[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-005571?style=for-the-badge&logo=kibana&logoColor=white)](https://www.elastic.co/kibana/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com/)

# 🚨 Real-Time Anomaly Detection Pipeline

> *Because manually checking logs is so 2019*

A streaming ML pipeline that catches network anomalies faster than you can say "zero-day exploit". Built with Kafka, scikit-learn, and enough caffeine to power a small datacenter.

## What It Does

- **Ingests**: Network events from 4 producers (variety is the spice of life)
- **Processes**: ML classification with Random Forest (it knows things)  
- **Visualizes**: Pretty Kibana dashboards that actually make sense
- **Scales**: Horizontally, because we're not monsters

## High Level Design

```mermaid
%%{init: {
  'theme': 'dark',
  'themeVariables': {
    'primaryColor': '#21262d',
    'primaryTextColor': '#f0f6fc',
    'primaryBorderColor': '#30363d',
    'lineColor': '#7d8590',
    'secondaryColor': '#161b22',
    'tertiaryColor': '#0d1117',
    'background': '#0d1117',
    'mainBkg': '#21262d',
    'secondBkg': '#1c2128',
    'fontFamily': 'SF Mono, Monaco, Inconsolata, Roboto Mono, monospace',
    'fontSize': '12px',
    'c0': '#58a6ff',
    'c1': '#56d364', 
    'c2': '#f2cc60',
    'c3': '#f85149',
    'c4': '#bc8cff'
  }
}}%%

graph LR
    subgraph "Sources"
        P1[Producer 1]
        P2[Producer 2] 
        P3[Producer 3]
        P4[Producer 4]
    end
    
    subgraph "Processing"
        K[Kafka Topic<br/>4 partitions]
        C1[Consumer 1]
        C2[Consumer 2]
        C3[Consumer 3]
        ML[Random Forest<br/>ML Model]
    end
    
    subgraph "Storage"
        ES[Elasticsearch]
    end
    
    subgraph "Visualization"
        KB[Kibana]
    end
    
    P1 --> K
    P2 --> K
    P3 --> K
    P4 --> K
    
    K --> C1
    K --> C2
    K --> C3
    
    C1 --> ML
    C2 --> ML
    C3 --> ML
    
    ML --> ES
    ES --> KB
```

## Quick Start

```bash
# The essentials
conda create -n anomaly-detection python=3.9 -y
conda activate anomaly-detection
pip install -r requirements.txt

# Wake up the containers
docker-compose up -d

# Create Kafka topic (4 partitions for maximum throughput)
docker exec -it kafka kafka-topics --create \
  --topic anomaly-events \
  --bootstrap-server kafka:9092 \
  --partitions 4 --replication-factor 1

# Start producers (in separate terminals, or use tmux like a pro)
python producers/producer1.py
python producers/producer2.py  
python producers/producer3.py
python producers/producer4.py

# Start consumers (the real MVPs)
python consumers/consumer1.py
python consumers/consumer2.py
python consumers/consumer3.py

# Admire your creation
open http://localhost:5601
```

## Architecture

```
📡 Producers → 🌊 Kafka → 🤖 ML Consumers → 📊 Elasticsearch → 📈 Kibana
```

- **4 Producers**: Different personalities, same goal
- **1 Kafka Topic**: 4 partitions for parallel processing magic
- **3 Consumers**: Load balanced like a well-designed API
- **Random Forest**: The AI that actually works
- **Elasticsearch**: Your data's new best friend
- **Kibana**: Makes ugly data beautiful


## ML Model

Detects these attack types:
- **Benign** (the boring stuff)
- **Phishing** (digital fishing, but ruder)
- **Malware** (software with commitment issues)
- **Port Scan** (digital door-knocking)

**Features**: IP addresses, byte counts, timestamps, and mathematical relationships that would make Pythagoras proud.

## Cleanup

```bash
# Nuclear option
rm -rf ./data ./logs ./elasticsearch-data ./kafka-data ./kibana-data

# Graceful shutdown  
docker-compose down -v
```

## Future Improvements

- Multi-broker Kafka (because SPOF is scary)
- Kubernetes deployment (enterprise swagger)
- Real-time alerting (3 AM notifications, anyone?)
- More ML models (because one is never enough)

**Pro tip**: In cybersecurity, paranoia isn't a bug—it's a feature! 🔐
