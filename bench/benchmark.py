#!/usr/bin/env python3
"""Repeatable throughput/scalability benchmark for the Flink trend job.

Preloads Kafka with the first N events of Monday's Trading Data once, then for each
parallelism level restarts the job from the earliest offset, waits until it has read
all N events and drained its sinks, and stores the run's Prometheus metrics
(Flink reporter + cAdvisor) as JSON under report/results/runs/.

Run from the dev shell with the Compose stack up:
    python3 bench/benchmark.py --events 20000000 --parallelism 1 2 4 8 --reps 3
"""
import argparse
import json
import math
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FLINK = "http://localhost:8081"
PROM = "http://localhost:9090"
DAY_FILE = ROOT / "data/debs2022-gc-trading-day-08-11-21.csv"
RESULTS = ROOT / "report/results"
PARTITIONS = 8
SLOTS_PER_TM = 2
SOURCE = 'operator_name="Source:_kafka_ticks"'
CONTAINERS = {
    "kafka": "kafka",
    "jobmanager": "flink-jobmanager",
    "taskmanagers": ".*taskmanager.*",
    "timescaledb": "timescaledb",
}


def sh(*cmd, env=None):
    return subprocess.run(cmd, check=True, text=True, capture_output=True, cwd=ROOT,
                          env={**os.environ, **(env or {})}).stdout


def http(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        text = resp.read()
    return json.loads(text) if text else None


def prom(query, at=None):
    params = {"query": query, **({"time": at} if at else {})}
    return http(f"{PROM}/api/v1/query?{urllib.parse.urlencode(params)}")["data"]["result"]


def prom_range(query, start, end, step=1):
    params = {"query": query, "start": start, "end": end, "step": step}
    result = http(f"{PROM}/api/v1/query_range?{urllib.parse.urlencode(params)}")["data"]["result"]
    return {json.dumps(r["metric"], sort_keys=True): [(float(t), float(v)) for t, v in r["values"]]
            for r in result}


def scalar(query, at=None):
    result = prom(query, at)
    return float(result[0]["value"][1]) if result else 0.0


def psql(sql):
    return sh("docker", "exec", "timescaledb", "psql", "-U", "trading", "-d", "trading", "-Atc", sql).strip()


def wait_for(predicate, timeout, interval=2, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if value := predicate():
            return value
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


def active_jobs():
    return [j["jid"] for j in http(f"{FLINK}/jobs/overview")["jobs"]
            if j["state"] not in ("CANCELED", "FINISHED", "FAILED")]


def cancel_jobs():
    for jid in active_jobs():
        http(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH")
    wait_for(lambda: not active_jobs(), 120, what="job cancellation")


def slice_file(events):
    """First `events` lines of Monday (plus the 12 header lines the producer skips)."""
    path = ROOT / f"data/bench-{events}.csv"
    if not path.exists():
        with open(DAY_FILE) as src, open(path, "w") as dst:
            for i, line in enumerate(src):
                if i >= events + 12:
                    break
                dst.write(line)
    return path


def preload(path):
    cancel_jobs()
    kafka_topics = ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server", "localhost:9092"]
    subprocess.run([*kafka_topics, "--delete", "--if-exists", "--topic", "ticks"], check=True,
                   capture_output=True)
    wait_for(lambda: "ticks" not in sh(*kafka_topics, "--list").split(), 60, what="topic deletion")
    out = sh("java", "-jar", "producer/target/producer.jar", str(path),
             env={"PARTITIONS": str(PARTITIONS)})
    sent, seconds, rate = re.search(r"sent (\d+) events in ([\d.,]+) s \((\d+) events/s\)", out).groups()
    return {"events": int(sent), "seconds": float(seconds.replace(",", ".")), "events_per_s": int(rate)}


def scale_taskmanagers(count):
    sh("docker", "compose", "up", "-d", "--no-deps", "--scale", f"taskmanager={count}", "taskmanager")
    wait_for(lambda: (o := http(f"{FLINK}/overview"))["taskmanagers"] == count
             and o["slots-available"] == o["slots-total"], 120, what=f"{count} taskmanagers with free slots")
    # Prometheus discovers the new replicas through DNS before the run starts
    wait_for(lambda: len(prom("up{job='flink-taskmanager'} == 1")) == count, 120, what="TM scrape targets")


def upload_jar(jar):
    out = sh("curl", "-sf", "-X", "POST", "-H", "Expect:", "-F", f"jarfile=@{jar}", f"{FLINK}/jars/upload")
    return Path(json.loads(out)["filename"]).name


def submit(jar_id, parallelism):
    jid = http(f"{FLINK}/jars/{jar_id}/run", "POST",
               {"entryClass": "trading.flink.TrendJob", "parallelism": parallelism})["jobid"]
    wait_for(lambda: all(v["status"] == "RUNNING" for v in http(f"{FLINK}/jobs/{jid}")["vertices"]),
             120, what="job running")
    return jid


def run_once(jar_id, parallelism, events):
    cancel_jobs()
    scale_taskmanagers(math.ceil(parallelism / SLOTS_PER_TM))
    psql("TRUNCATE ema_results, crossover_events")
    t_submit = time.time()
    jid = submit(jar_id, parallelism)
    t_running = time.time()
    job = f'job_id="{jid}"'
    # Records the Kafka consumer fetched, including those the deserializer then skips
    read = f"sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{{{SOURCE},{job}}})"
    wait_for(lambda: scalar(read) >= events, 3600, interval=2, what="source to read all events")
    t_read = time.time()

    # Sinks are drained once the DB row counts stop changing for two scrapes
    def rows():
        return psql("SELECT (SELECT count(*) FROM ema_results), (SELECT count(*) FROM crossover_events)")
    last = [None]

    def stable():
        now = rows()
        done, last[0] = now == last[0], now
        return done
    wait_for(stable, 600, interval=6, what="sinks to drain")
    t_drained = time.time()
    ema_rows, crossover_rows = map(int, last[0].split("|"))

    start, end = t_running - 10, t_drained + 5
    series = {
        "records_read": prom_range(read, start, end),
        "read_rate": prom_range(
            f"sum(rate(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{{{SOURCE},{job}}}[15s]))", start, end),
        "rows_written_rate": prom_range(
            f'sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{{operator_name=~"timescaledb_ema_results.*",{job}}}[15s]))',
            start, end),
        "busy_ms_per_s": prom_range(
            f"avg by (task_name) (flink_taskmanager_job_task_busyTimeMsPerSecond{{{job}}})", start, end),
        # Time Kafka partitions are paused by watermark alignment
        "paused_ms_per_s": prom_range(
            f"avg(flink_taskmanager_job_task_operator_split_watermark_pausedTimeMsPerSecond{{{job}}})", start, end),
        "backpressured_ms_per_s": prom_range(
            f"avg by (task_name) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond{{{job}}})", start, end),
        **{f"cpu_cores:{name}": prom_range(
            f'sum(rate(container_cpu_usage_seconds_total{{name=~"{regex}"}}[15s]))', start, end)
           for name, regex in CONTAINERS.items()},
        **{f"memory_bytes:{name}": prom_range(
            f'sum(container_memory_working_set_bytes{{name=~"{regex}"}})', start, end)
           for name, regex in CONTAINERS.items()},
    }
    # Events read per source subtask, i.e. per Kafka partition group, to quantify skew
    per_subtask = [float(r["value"][1]) for r in prom(
        f"flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{{{SOURCE},{job}}}", t_read)]
    restarts = scalar(f"flink_jobmanager_job_numRestarts{{{job}}}")
    late = scalar(f"sum(flink_taskmanager_job_task_operator_numLateRecordsDropped{{{job}}})")
    # Jobs that filter after the source count price events at the Filter; the final job's source emits only them
    price = scalar(f'sum(flink_taskmanager_job_task_operator_numRecordsOut{{operator_name="Filter",{job}}}) '
                   f'or sum(flink_taskmanager_job_task_operator_numRecordsOut{{{SOURCE},{job}}})')
    return {
        "parallelism": parallelism, "taskmanagers": math.ceil(parallelism / SLOTS_PER_TM), "job_id": jid,
        "events": events, "t_submit": t_submit, "t_running": t_running, "t_read": t_read,
        "t_drained": t_drained, "restarts": restarts, "read_per_subtask": per_subtask, "price_events": price, "late_dropped": late,
        "ema_rows": ema_rows, "crossover_rows": crossover_rows, "series": series,
    }


def dump(label):
    """Exports the run's query results for bench/reference.py."""
    out = ROOT / "data/results"
    out.mkdir(exist_ok=True)
    for table, cols in [("ema_results", "last_price, ema38, ema100"), ("crossover_events", "signal_type")]:
        sql = (f"COPY (SELECT symbol, extract(epoch FROM window_end)::bigint, {cols} FROM {table} "
               f"ORDER BY 1, 2) TO STDOUT WITH CSV")
        (out / f"{label}-{table}.csv").write_text(psql(sql) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--events", type=int, default=20_000_000)
    parser.add_argument("--parallelism", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--input", help="CSV to preload instead of the first --events Monday events")
    parser.add_argument("--jar", default="flink-job/target/flink-job.jar")
    parser.add_argument("--label", default="main", help="prefix for result files")
    parser.add_argument("--skip-load", action="store_true", help="reuse the events already in Kafka")
    parser.add_argument("--dump", action="store_true", help="export the first run per parallelism for reference.py")
    args = parser.parse_args()

    runs_dir = RESULTS / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    source = Path(args.input) if args.input else ROOT / f"data/bench-{args.events}.csv"
    producer_file = RESULTS / f"producer-{source.stem}.json"
    if not args.skip_load:
        load = preload(source if args.input else slice_file(args.events))
        producer_file.write_text(json.dumps(load, indent=2))
        print(f"preloaded {load}", flush=True)
    # The producer's count is exact; the slice may contain a few non-event lines
    events = json.loads(producer_file.read_text())["events"]
    jar_id = upload_jar(ROOT / args.jar)
    for parallelism in args.parallelism:
        for rep in range(args.reps):
            run = run_once(jar_id, parallelism, events)
            run["label"], run["rep"] = args.label, rep
            (runs_dir / f"{args.label}-p{parallelism}-r{rep}.json").write_text(json.dumps(run))
            print(f"{args.label} p={parallelism} rep={rep}: read in {run['t_read'] - run['t_running']:.0f}s, "
                  f"drained in {run['t_drained'] - run['t_running']:.0f}s, late={run['late_dropped']:.0f}, "
                  f"ema_rows={run['ema_rows']}, restarts={run['restarts']:.0f}", flush=True)
            if args.dump and rep == 0:
                dump(f"{args.label}-p{parallelism}")
    cancel_jobs()


if __name__ == "__main__":
    main()
