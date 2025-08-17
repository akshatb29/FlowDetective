# 🚨 Real-Time Anomaly Detection Pipeline

> *"Finding needles in haystacks, but the needles are hackers and the haystack is your network traffic"*

[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-005571?style=for-the-badge&logo=kibana&logoColor=white)](https://www.elastic.co/kibana/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com/)

An end-to-end streaming system that ingests network events faster than your morning coffee kicks in, classifies potential attacks using machine learning, and serves up beautiful visualizations that would make even Grafana jealous.

graph TB
    subgraph "Data Sources"
        P1[Producer 1<br/>Network Devices]
        P2[Producer 2<br/>Security Tools]
        P3[Producer 3<br/>Application Logs]
        P4[Producer 4<br/>Infrastructure]
    end
    
    subgraph "Stream Processing Layer"
        K[Apache Kafka<br/>anomaly-events topic<br/>4 partitions]
        CG[Consumer Group<br/>anomaly-detector]
        C1[Consumer 1<br/>ML Inference]
        C2[Consumer 2<br/>ML Inference] 
        C3[Consumer 3<br/>ML Inference]
    end
    
    subgraph "ML Processing"
        RF[Random Forest<br/>Classifier<br/>5 Classes]
        FE[Feature<br/>Engineering]
    end
    
    subgraph "Storage & Search"
        ES[Elasticsearch<br/>anomalies index]
        ESM[ES Master Node]
        ESD1[ES Data Node 1]
        ESD2[ES Data Node 2]
    end
    
    subgraph "Visualization & Monitoring"
        KB[Kibana<br/>Dashboards]
        AL[Alerting<br/>System]
        MT[Metrics<br/>Collection]
    end
    
    subgraph "Service Health"
        HL[Health Logs<br/>service_logs topic]
        HM[Health<br/>Monitoring]
    end
    
    P1 --> K
    P2 --> K
    P3 --> K
    P4 --> K
    
    K --> CG
    CG --> C1
    CG --> C2
    CG --> C3
    
    C1 --> FE --> RF
    C2 --> FE
    C3 --> FE
    
    RF --> ES
    ES --> ESM
    ESM --> ESD1
    ESM --> ESD2
    
    ES --> KB
    ES --> AL
    
    C1 --> HL
    C2 --> HL
    C3 --> HL
    HL --> HM
    
    KB --> MT
    AL --> MT

## 🎯 What Does This Thing Actually Do?

This pipeline is like having a digital security guard that never sleeps, never takes coffee breaks, and definitely doesn't spend time scrolling social media. It:

- **Ingests** network events from multiple producers (because one is never enough)
- **Processes** them through a Random Forest classifier that's smarter than your average bear
- **Visualizes** anomalies in real-time dashboards that actually make sense
- **Scales** horizontally because nobody has time for bottlenecks

Perfect for impressing your manager, acing technical interviews, or just having something cool to show at parties (if you're into that kind of party).

## ✨ Key Features

- **4 Producers** → Because diversity is beautiful, even in data generation
- **3 Consumers** → Load balancing so smooth it makes butter jealous  
- **ML-Powered Detection** → Random Forest algorithm that actually knows the forest from the trees
- **Real-time Visualization** → Kibana dashboards prettier than a sunset
- **Fault Tolerance** → Survives crashes better than a Nokia 3310

## 🏗️ Architecture Overview

```
📡 Producers (4x) → 🌊 Kafka Stream → 🤖 ML Consumers (3x) → 📊 Elasticsearch → 📈 Kibana
```

**The Players:**
- **Kafka**: The reliable messenger that never loses your mail
- **Random Forest ML Model**: The digital Sherlock Holmes
- **Elasticsearch**: The librarian with perfect memory
- **Kibana**: The artist that makes data beautiful

## 🧠 How Smart Is This Thing?

Our ML model detects these attack types:
- **Benign** - The good guys (boring, but necessary)
- **Phishing** - Digital fishing, but they're not after fish 🎣
- **Malware** - Software with trust issues
- **Port Scan** - Digital door-knocking, but ruder
- **Zero-Day Exploit** - The "oops, that shouldn't exist" category

**Features Used:**
- Network byte counts (size matters)
- IP address intelligence (geography is destiny)  
- Temporal patterns (timing is everything)
- Mathematical relationships (because math never lies)

## 🚀 Quick Start Guide

### Prerequisites
- Docker & Docker Compose (your containerization BFFs)
- Python 3.9+ (because we're not living in the stone age)
- A cup of coffee ☕ (optional but recommended)

### 1. Environment Setup
```bash
# Create your Python sanctuary
conda create -n anomaly-detection python=3.9 -y
conda activate anomaly-detection
pip install -r requirements.txt
```

### 2. Launch the Fleet
```bash
# Wake up the containers
docker-compose up -d

# Create Kafka topic (4 partitions for maximum throughput)
docker exec -it kafka kafka-topics --create \
  --topic anomaly-events \
  --bootstrap-server kafka:9092 \
  --partitions 4 --replication-factor 1
```

### 3. Start the Data Generators
```bash
# In separate terminals (or tmux if you're fancy)
python producers/producer1.py  # The steady Eddie
python producers/producer2.py  # The anomaly generator
python producers/producer3.py  # The metadata enthusiast  
python producers/producer4.py  # The bursty one
```

### 4. Deploy the Consumers
```bash
python consumers/consumer1.py  # Consumer extraordinaire
python consumers/consumer2.py  # The reliable backup
python consumers/consumer3.py  # Third time's the charm
```

### 5. Admire Your Creation
Open [http://localhost:5601](http://localhost:5601) and prepare to be amazed by your data visualization prowess.

## 📊 Dashboard Creation Guide

Once in Kibana, create these visualizations to become the Picasso of cybersecurity data:

1. **Anomaly Distribution Pie Chart**
   - Type: Pie
   - Field: `predicted_attack`
   - Because everyone loves pie 🥧

2. **Attack Timeline**
   - Type: Line chart  
   - X-axis: Timestamp
   - Series: Split by `predicted_attack`
   - Watch attacks flow like a digital river

3. **Top Talkers Bar Chart**
   - Type: Bar
   - Dimension: `src_ip`
   - Metric: Sum of `bytes_sent`
   - Find your chatty network neighbors

4. **Total Anomaly Counter**
   - Type: Metric
   - Filter: NOT `predicted_attack:"Benign"`
   - Big numbers make everyone happy

## 🔍 Load Balancing Demo (Impress Your Interviewer)

Want to show off? Here's how to demonstrate automatic partition rebalancing:

```bash
# Check current partition assignments
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group anomaly-detector \
  --describe

# Kill a consumer (dramatic effect)
# Watch partitions automatically reassign
# Bring consumer back online
# Marvel at the self-healing magic ✨
```

## 📁 Project Structure
```
anomaly-pipeline/
├── 🐳 docker-compose.yml          # Container orchestration magic
├── 📡 producers/                  # Data generation squad
│   ├── producer1.py              # The baseline hero
│   ├── producer2.py              # The anomaly artist
│   ├── producer3.py              # The metadata maven
│   └── producer4.py              # The burst specialist
├── 🤖 consumers/                  # Processing powerhouses
│   ├── consumer1.py              # Primary processor
│   ├── consumer2.py              # Backup brain
│   └── consumer3.py              # Triple redundancy
├── 🧠 models/                     # ML intelligence
│   └── rf_model.pkl              # The trained sage
├── 📋 requirements.txt            # Dependency declarations
└── 📖 README.md                   # This masterpiece
```

## 🔧 Event Schemas

### Anomaly Events (The Main Course)
```json
{
  "timestamp": "2025-08-17 20:08:26",
  "src_ip": "192.168.0.42",
  "dst_ip": "10.0.0.105", 
  "bytes_sent": 163,
  "meta": {
    "producer_id": "3",
    "burst": false,
    "note": "Producer-specific wisdom"
  }
}
```

### Service Heartbeats (Proof of Life)
```json
{
  "node_id": "consumer-1",
  "timestamp": "2025-08-17T20:09:00.123456",
  "message_type": "HEARTBEAT",
  "processed_count": 128
}
```

## 📈 Metrics That Matter

Keep an eye on these numbers to stay sane:

- **Ingress Rate**: Messages per second (higher = better coffee)
- **Consumer Throughput**: Processing speed per consumer
- **Partition Lag**: How far behind we are (lower = happier)  
- **Heartbeat Frequency**: Proof your consumers are alive
- **Anomaly Distribution**: What attacks are trending

## 🧹 Cleanup Commands

Because nobody likes digital clutter:

```bash
# Nuclear option - clean everything
rm -rf ./data ./logs ./elasticsearch-data ./kafka-data ./kibana-data

# Graceful shutdown
docker-compose down -v
```

## 🚀 Future Enhancements (Dream Big)

- **Multi-broker Kafka** - Because single points of failure are so last year
- **Kubernetes Deployment** - For that enterprise-grade swagger
- **Schema Registry** - Type safety for the data-obsessed
- **Dead Letter Queues** - Where bad messages go to think about what they've done
- **Prometheus + Grafana** - Infrastructure monitoring that sparks joy
- **Slack Alerts** - Because who doesn't want attack notifications at 3 AM?

## 🤝 Contributing

Found a bug? Have an idea? Think the documentation needs more emojis? We welcome contributions! Just remember:

1. Fork it 🍴
2. Create a feature branch 🌿  
3. Commit your changes 💾
4. Push to the branch 🚀
5. Open a Pull Request 📬

## 📄 License

This project is licensed under the "Do Whatever You Want But Don't Blame Us" License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Coffee** ☕ - For making this possible
- **Stack Overflow** - For answering questions we didn't know we had
- **The Apache Foundation** - For building tools that actually work
- **Elastic** - For making search not suck
- **Our Future Selves** - For hopefully maintaining this code

---

*Made with 💻, 🧠, and a questionable amount of ☕ by developers who believe security shouldn't be boring.*

**Remember**: In the world of cybersecurity, paranoia is just good planning! 🕵️‍♀️
