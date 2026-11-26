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

To start Kafka, the Flink cluster, TimescaleDB, the web dashboard, cAdvisor, Prometheus, and Grafana in the background, run:

```bash
docker compose up -d
```

Kafka is reachable at `kafka:9092` from containers and `localhost:9094` from the host. TimescaleDB (user/password/db `trading`) listens on host port 5432 (override with `TIMESCALEDB_PORT`). The web dashboard is at [http://localhost:7070](http://localhost:7070), Prometheus is at [http://localhost:9090](http://localhost:9090), and Grafana is at [http://localhost:3000](http://localhost:3000) with `admin`/`admin` credentials.

Prometheus scrapes the web API, the Flink JobManager and every TaskManager replica through Flink's Prometheus reporter (port 9249), and per-container CPU and memory from cAdvisor ([http://localhost:8080](http://localhost:8080)).

The SQL files in `db/init/` create the `ema_results` and `crossover_events` Timescale hypertables, their indexes, and `latest_*` views. Docker applies it when the database volume is first created. To reapply it to a local database, reset the project volumes with `docker compose down -v` before starting the stack again.

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

`docker compose up` builds the job and submits it through the `flink-job-submitter` service, with parallelism `PARALLELISM` (default 1). The job reads topic `ticks` from the earliest offset, so it restarts until the producer has created the topic. To resubmit by hand after a change:

```bash
mvn -pl flink-job -am package
docker cp flink-job/target/flink-job.jar flink-jobmanager:/opt/flink/flink-job.jar
docker exec flink-jobmanager /opt/flink/bin/flink run -p 1 \
	-c trading.flink.TrendJob \
	/opt/flink/flink-job.jar
```

## Benchmarks and Report

`bench/` holds the repeatable performance evaluation behind `report/`. With the stack up and the day CSVs in `data/`, from the dev shell (`nix develop`):

```bash
mvn -pl producer,flink-job -am package
# Scalability: first 20M Monday events, 3 runs per parallelism
python3 bench/benchmark.py --events 20000000 --parallelism 1 2 4 8 --reps 3 --dump
python3 bench/reference.py data/bench-20000000.csv main-p1 main-p2 main-p4 main-p8
# Correctness: every price event of the week
for f in data/debs2022-gc-trading-day-*.csv; do LC_ALL=C awk -F, '$22!="" && $1!~/^#/ && $1!="ID"' "$f"; done > data/week-prices.csv
python3 bench/benchmark.py --input data/week-prices.csv --parallelism 4 8 --reps 1 --dump --label week
python3 bench/reference.py data/week-prices.csv week-p4 week-p8
python3 bench/plot.py
typst compile --font-path report/fonts report/main.typ
```

`benchmark.py` preloads Kafka (8 partitions), then for each parallelism scales the TaskManagers to `parallelism / 2`, runs the job from the earliest offset until it has read every event and drained its sinks, and saves the Flink and cAdvisor series from Prometheus to `report/results/runs/`. `--dump` exports the first run per parallelism from TimescaleDB to `data/results/`. `reference.py` recomputes both queries single-threaded straight from the CSV, under batch and streaming semantics, and compares them with those exports. `plot.py` writes the figures to `report/figures/` and the summary the report reads to `report/results/summary.json`.

The report's "no fix" control is the same job with the watermark strategy applied after the Kafka source instead of in it; build that variant and pass its jar with `--jar` and `--label prefix` (Monday) or `--label weekprefix` (week).
