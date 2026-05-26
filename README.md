# Distributed Logging System

Microservices architecture with centralized log collection, real-time streaming via Kafka, storage in Elasticsearch, and live alerting via a Flask/SocketIO dashboard.


## Architecture

![Architecture Diagram](https://github.com/user-attachments/assets/010c4221-955c-411c-88e5-efdf86bae1b7)

### Components

| Component | Role |
|-----------|------|
| **Microservices** | Generate logs and heartbeat signals |
| **Log Accumulator** | Collects and forwards logs to Fluentd |
| **Fluentd** | Aggregates and routes logs/heartbeats to Kafka |
| **Kafka** | Pub-Sub backbone for async log distribution |
| **Elasticsearch** | Indexes and stores logs for querying |
| **PUB-SUB / Flask app** | Consumes Kafka topics, detects failures, drives alerting UI |
| **Kibana** | Log visualization dashboard |

### Flow

```
Microservices → Log Accumulator → Fluentd → Kafka → PUB-SUB consumer → Elasticsearch
                                                                       → Alerting UI (port 5000)
```

---

## Quick Start (Docker)

**Requires**: Docker, Docker Compose

```bash
git clone https://github.com/Cloud-Computing-Big-Data/RR-Team-48-distributed-logging-system.git
cd RR-Team-48-distributed-logging-system
docker compose up --build
```

Services started:

| Service | URL |
|---------|-----|
| Alerting dashboard | http://localhost:5000 |
| Elasticsearch | http://localhost:9200 |
| Fluentd (forward input) | localhost:9880 |
| Kafka | localhost:9092 |

### Environment Variables

All addresses are configurable via environment variables (defaults work for Docker Compose):

| Variable | Default | Used by |
|----------|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | PUB-SUB, Fluentd |
| `ES_HOST` | `http://localhost:9200` | PUB-SUB |
| `FLUENTD_HOST` | `localhost` | All microservices |
| `FLUENTD_PORT` | `9880` | All microservices |

---

## Manual Setup

### Prerequisites

- Python 3.12+
- Kafka + Zookeeper
- Fluentd with `fluent-plugin-kafka`
- Elasticsearch 8.x
- Kibana (optional, for visualization)

### Install Python dependencies

```bash
pip install -r requirements.txt
```

### Fluentd

**Ubuntu/Debian:**
```bash
sudo apt-get update && sudo apt-get install -y sudo gnupg2 curl
curl -fsSL https://packages.fluentd.org/fluentd-apt-source.sh | sudo bash
sudo apt-get install -y fluentd
gem install fluent-plugin-kafka --no-document
```

**macOS:**
```bash
brew install fluentd
gem install fluent-plugin-kafka --no-document
```

Apply the project config:
```bash
chmod +x update_conf.sh
./update_conf.sh
```

### Kafka

1. Download from [kafka.apache.org/downloads](https://kafka.apache.org/downloads) and extract:
   ```bash
   tar -xvf kafka_2.13-3.x.x.tgz
   cd kafka_2.13-3.x.x
   ```

2. Add to `config/server.properties`:
   ```properties
   listeners=PLAINTEXT://0.0.0.0:9092
   advertised.listeners=PLAINTEXT://<your-hostname>:9092
   ```

### Elasticsearch

```bash
sudo apt-get update
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
sudo apt-get install apt-transport-https
echo "deb https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list
sudo apt-get update && sudo apt-get install elasticsearch
sudo systemctl enable elasticsearch && sudo systemctl start elasticsearch
curl localhost:9200
```

### Kibana

```bash
sudo apt-get install kibana
sudo systemctl enable kibana && sudo systemctl start kibana
```

### Start all services

```bash
sudo systemctl start elasticsearch
sudo systemctl start kibana
sudo systemctl start fluentd
# start Zookeeper then Kafka per your Kafka install method
```

### Run microservices

```bash
python inventory_service/inventory_service.py
python order_service/order_service.py
python payment_service/payment_service.py
python shipping_service/shipping_service.py
python PUB-SUB/app.py
```

---

## Features

- **Centralized log management** — all services log to one pipeline
- **Real-time processing** — Kafka streams logs with sub-second latency
- **Heartbeat monitoring** — detects service failures when heartbeats stop
- **Live alerting** — Flask/SocketIO UI flags ERROR/FATAL events instantly
- **Queryable storage** — Elasticsearch indexes enable fast log search
- **Containerized** — full stack runs with a single `docker compose up`

---

## Screenshots

**Running microservices (payment service example):**
![Payment service](https://github.com/user-attachments/assets/228d6180-28e4-44ba-bfac-08c3c31bedf0)

**PUB-SUB consumer:**
![PUB model 1](https://github.com/user-attachments/assets/c765e998-b7c9-4a62-82cc-5257c922f864)
![PUB model 2](https://github.com/user-attachments/assets/e2ec6898-bec5-4dbe-a8dd-95318295e34e)
![PUB model 3](https://github.com/user-attachments/assets/72cbba22-b2fb-4575-b1d1-466cf36274f5)

**Alerting UI:**
![Alerting UI](https://github.com/user-attachments/assets/f1bea336-1b08-41ce-bd57-704d6deb3059)

**Kibana visualization:**
![Kibana](https://github.com/user-attachments/assets/9be5223a-12a3-4558-9243-936bdb34f2a7)

---

## Future Improvements

- Distributed tracing with Jaeger
- Anomaly detection on log patterns
- Log retention and archiving policies

---

## References

- [Fluentd Documentation](https://docs.fluentd.org/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/index.html)
- [Kibana Documentation](https://www.elastic.co/guide/en/kibana/current/index.html)
