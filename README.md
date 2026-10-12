# Scalable Real-Time Trading Trend Detection

This repository contains the foundational infrastructure for the trading trend detection system.

## Prerequisites

- Docker
- Docker Compose

## Start the Cluster

To start the Kafka broker and Flink cluster in the background, run:

```bash
docker compose up -d
```

## Verify Health

### Flink
Check the Flink JobManager dashboard by opening your browser to:
[http://localhost:8081](http://localhost:8081)

You should see 1 Task Manager and 2 Available Task Slots.

### Kafka
Prove the Kafka broker is up and responsive by listing its topics from within the container:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

*(This command will return successfully with empty output since no topics exist yet).*
