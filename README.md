# Scalable Real-Time Trading Trend Detection

This repository contains the foundational infrastructure for the trading trend detection system.

## Layout

Maven multi-module project. JVM parts run on the host, infrastructure runs in Docker Compose.

| Module      | Purpose                                                 |
| ----------- | ------------------------------------------------------- |
| `common`    | Shared `TickEvent` schema and JSON serde                |
| `producer`  | Replays the Trading Data CSVs into Kafka topic `ticks`  |
| `flink-job` | Query 1 (EMA) and Query 2 (crossovers)                  |
| `web`       | TimescaleDB API and the trading terminal frontend       |

## Prerequisites

- Docker
- Docker Compose
- JDK 21+ (or `nix develop` / direnv, which provides it)
- Maven 3.9+
- Node.js 22+ and npm, to build the web frontend (not needed for `docker compose`,
  or pass `-Dskip.frontend` to Maven)

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

## Web Terminal

The terminal at [http://localhost:7070](http://localhost:7070) shows, for the
selected symbol, the price with EMA 38 and EMA 100, BUY/SELL markers at each
crossover and the EMA spread below the chart. Around it are a watchlist (stored
in the browser), a live feed of the latest advisories across all symbols, and a
symbol search (press `/`). Times are shown in exchange-local time.

The frontend lives in `web/frontend` (Vite, React, TypeScript, and TradingView
Lightweight Charts). `mvn package` runs `npm ci` and `npm run build`, which
writes the bundle into the web jar, so the Java server serves everything.

For frontend work, run the API (for example with `docker compose up -d
timescaledb web`) and start the dev server with hot reload:

```bash
cd web/frontend
npm install
npm run dev        # http://localhost:5173, proxies /api to localhost:7070
```

To work on the UI without replaying the data set, load synthetic results into
an empty development database. The script refuses to run if `ema_results` or
`crossover_events` already has rows; adding `--force` replaces their contents:

```bash
node web/frontend/scripts/seed-demo.mjs | docker exec -i timescaledb psql -q -U trading -d trading
```

The web API endpoints are:

| Endpoint                       | Purpose                                                      |
| ------------------------------ | ------------------------------------------------------------ |
| `/api/symbols`                 | Lists symbols with stored EMA or price data                  |
| `/api/summary`                 | Newest window end and total BUY/SELL counts                  |
| `/api/watchlist?symbols=A,B`   | Latest rows, recent prices and last signal for given symbols |
| `/api/trends/latest`           | Returns the latest result rows                               |
| `/api/trends/{symbol}?hours=N` | Returns historical rows for one symbol                       |
| `/api/signals/latest`          | Returns the latest crossover advisories                      |
| `/api/signals/{symbol}?hours=N`| Returns crossover advisories for one symbol                  |
| `/health`                      | Checks database connectivity                                 |
| `/metrics`                     | Exposes Prometheus metrics                                   |

## Run the Flink Job

Build the Flink job and submit it to the JobManager:

```bash
mvn -pl flink-job -am package
docker cp flink-job/target/flink-job.jar flink-jobmanager:/opt/flink/flink-job.jar
docker exec flink-jobmanager /opt/flink/bin/flink run \
	-c trading.flink.TrendJob \
	/opt/flink/flink-job.jar
```
