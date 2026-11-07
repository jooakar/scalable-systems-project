# Scalable Real-Time Trading Trend Detection

This repository contains the foundational infrastructure for the trading trend detection system.

## Layout

Maven multi-module project. JVM parts run on the host, infrastructure runs in Docker Compose.

| Module      | Purpose                                                 |
| ----------- | ------------------------------------------------------- |
| `common`    | Shared `TickEvent` schema and JSON serde                |
| `producer`  | Replays the Trading Data CSVs into Kafka topic `ticks`  |
| `flink-job` | Query 1 (EMA) and Query 2 (crossovers), not implemented |
| `web`       | Backend serving TimescaleDB API and live EMA dashboard  |

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

To start Kafka, the Flink cluster, TimescaleDB, the web dashboard, Prometheus, and Grafana in the background, run:

```bash
docker compose up -d
```

Kafka is reachable at `kafka:9092` from containers and `localhost:9094` from the host. TimescaleDB (user/password/db `trading`) listens on host port 5432. The web dashboard is at [http://localhost:7070](http://localhost:7070), Prometheus is at [http://localhost:9090](http://localhost:9090), and Grafana is at [http://localhost:3000](http://localhost:3000) with `admin`/`admin` credentials.

The SQL file in `db/init/` creates the `ema_results` Timescale hypertable, indexes, and `latest_ema_results` view. Docker applies it when the database volume is first created. To reapply it to a local database, reset the project volumes with `docker compose down -v` before starting the stack again.

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

_(This command will return successfully with empty output since no topics exist yet)._

## Data

Download the day files into `data/` (gitignored)

## Run the Producer

```bash
mvn -pl producer -am package
java -jar producer/target/producer.jar data/
```

Arguments are CSV files or directories (a directory's files are replayed in name order). Every event is sent to Kafka keyed by symbol ID, slimmed to the attributes the project needs. Settings via environment variables:

| Variable            | Default          | Description                       |
| ------------------- | ---------------- | --------------------------------- |
| `BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka bootstrap servers           |
| `TOPIC`             | `ticks`          | Target topic (created if missing) |
| `PARTITIONS`        | `4`              | Partitions when creating topic    |
| `RATE`              | `0`              | Max events/s, `0` = unlimited     |

## Run the Flink Job

Build the Flink job and submit it to the JobManager:

```bash
mvn -pl flink-job -am package
docker cp flink-job/target/flink-job.jar flink-jobmanager:/opt/flink/flink-job.jar
docker exec flink-jobmanager /opt/flink/bin/flink run \
	-c trading.flink.TrendJob \
	/opt/flink/flink-job.jar
```

The job reads `ticks`, calculates one EMA-38/EMA-100 result per symbol and five-minute event-time window, and writes each result to TimescaleDB. The dashboard refreshes its latest-symbol table and chart every two seconds.
