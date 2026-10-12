# Scalable Real-Time Trading Trend Detection

This repository contains the foundational infrastructure for the trading trend detection system.

## Layout

Maven multi-module project. JVM parts run on the host, infrastructure runs in Docker Compose.

| Module      | Purpose                                                   |
|-------------|-----------------------------------------------------------|
| `common`    | Shared `TickEvent` schema and JSON serde                  |
| `producer`  | Replays the Trading Data CSVs into Kafka topic `ticks`    |
| `flink-job` | Query 1 (EMA) and Query 2 (crossovers), not implemented   |
| `web`       | Backend serving results from TimescaleDB, not implemented |

## Prerequisites

- Docker
- Docker Compose
- JDK 21+ (or `nix develop` / direnv, which provides it)
- Maven 3.9+

## Build

```bash
mvn verify         # build all modules and run tests
```

## Start the Cluster

To start Kafka, the Flink cluster and TimescaleDB in the background, run:

```bash
docker compose up -d
```

Kafka is reachable at `kafka:9092` from containers and `localhost:9094` from the host. TimescaleDB (user/password/db `trading`) listens on host port 5432, set `TIMESCALEDB_PORT` to use another port if 5432 is taken.

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

## Data

Download the day files into `data/` (gitignored)

## Run the Producer

```bash
mvn -pl producer -am package
java -jar producer/target/producer.jar data/
```

Arguments are CSV files or directories (a directory's files are replayed in name order). Every event is sent to Kafka keyed by symbol ID, slimmed to the attributes the project needs. Settings via environment variables:

| Variable            | Default          | Description                       |
|---------------------|------------------|-----------------------------------|
| `BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka bootstrap servers           |
| `TOPIC`             | `ticks`          | Target topic (created if missing) |
| `PARTITIONS`        | `4`              | Partitions when creating topic    |
| `RATE`              | `0`              | Max events/s, `0` = unlimited     |
