#!/usr/bin/env python3
"""End-to-end result latency of the Flink trend job under a steady input rate.

For each rate, starts the job on an empty topic, replays a CSV at that rate with the producer
and records when each window could first be evaluated: when the first price event at or after
its end was sent (WINDOW_LOG). A window's latency is the time from then until its EMA row was
committed in TimescaleDB (received_at), so it includes the 5 s out-of-orderness bound, waiting
for the slowest partition, processing and the batched commit. Writes report/results/latency.json.

Run from the dev shell with the Compose stack up:
    python3 bench/latency.py --parallelism 4
"""
import argparse
import json
import statistics
import subprocess
import time

from benchmark import (PARTITIONS, RESULTS, ROOT, SLOTS_PER_TM, cancel_jobs, psql,
                       scale_taskmanagers, sh, slice_file, submit, upload_jar, wait_for)

KAFKA_TOPICS = ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092"]


def fresh_topic():
    subprocess.run([*KAFKA_TOPICS, "--delete", "--if-exists", "--topic", "ticks"], check=True, capture_output=True)
    wait_for(lambda: "ticks" not in sh(*KAFKA_TOPICS, "--list").split(), 60, what="topic deletion")
    sh(*KAFKA_TOPICS, "--create", "--topic", "ticks", "--partitions", str(PARTITIONS))


def clock_offset_ms(samples=5):
    """Database clock minus host clock, from the sample with the shortest round trip."""
    best = None
    for _ in range(samples):
        before = time.time() * 1000
        db = float(psql("SELECT extract(epoch FROM clock_timestamp()) * 1000"))
        after = time.time() * 1000
        if best is None or after - before < best[1]:
            best = (db - (before + after) / 2, after - before)
    return {"offset_ms": best[0], "round_trip_ms": best[1]}


def percentile(values, q):
    return statistics.quantiles(values, n=1000, method="inclusive")[round(q * 1000) - 1]


def summarize(latencies):
    return {"p50": percentile(latencies, 0.5), "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99), "max": max(latencies)}


def run(jar_id, parallelism, path, rate):
    cancel_jobs()
    fresh_topic()
    psql("TRUNCATE ema_results, crossover_events")
    submit(jar_id, parallelism)
    log = ROOT / "data/window-log.csv"
    out = sh("java", "-jar", "producer/target/producer.jar", str(path),
             env={"PARTITIONS": str(PARTITIONS), "RATE": str(rate), "WINDOW_LOG": str(log)})
    sent = int(out.rsplit("sent ", 1)[1].split()[0])
    produced_s = float(out.rsplit(" in ", 1)[1].split()[0].replace(",", "."))

    last = [None]

    def stable():
        now = psql("SELECT count(*) FROM ema_results")
        done, last[0] = now == last[0], now
        return done
    wait_for(stable, 600, interval=6, what="sink to drain")
    clock = clock_offset_ms()

    opened = dict(tuple(map(int, line.split(","))) for line in log.read_text().split())
    rows = [tuple(map(float, r.split("|"))) for r in psql(
        "SELECT extract(epoch FROM window_end) * 1000, extract(epoch FROM received_at) * 1000 "
        "FROM ema_results").split()]
    per_row = [received - clock["offset_ms"] - opened[int(end)] for end, received in rows if int(end) in opened]
    per_window = {}
    for end, received in rows:
        if int(end) in opened:
            per_window[end] = max(per_window.get(end, 0), received - clock["offset_ms"] - opened[int(end)])
    return {
        "rate": rate, "parallelism": parallelism, "input": path.name, "events": sent,
        "achieved_rate": sent / produced_s, "rows": len(rows), "rows_matched": len(per_row),
        "windows": len(per_window), "clock": clock,
        "row_latency_ms": summarize(per_row), "window_latency_ms": summarize(list(per_window.values())),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--jar", default="flink-job/target/flink-job.jar")
    args = parser.parse_args()
    # Low rates replay a shorter slice so every run takes a minute or two. Running next to the job,
    # the producer tops out near 760k events/s, so the last rate is its maximum.
    plan = [(25_000, 2_000_000), (250_000, 20_000_000), (500_000, 20_000_000), (1_000_000, 20_000_000)]
    scale_taskmanagers(-(-args.parallelism // SLOTS_PER_TM))
    jar_id = upload_jar(ROOT / args.jar)
    results = []
    for rate, events in plan:
        result = run(jar_id, args.parallelism, slice_file(events), rate)
        results.append(result)
        print(f"rate={rate} achieved={result['achieved_rate']:.0f} windows={result['windows']} "
              f"rows={result['rows_matched']}/{result['rows']} row p50/p99/max="
              f"{result['row_latency_ms']['p50']:.0f}/{result['row_latency_ms']['p99']:.0f}/"
              f"{result['row_latency_ms']['max']:.0f} ms clock={result['clock']}", flush=True)
    cancel_jobs()
    (RESULTS / "latency.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
