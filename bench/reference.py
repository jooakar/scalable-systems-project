#!/usr/bin/env python3
"""Independent single-threaded reference for Query 1 and Query 2, compared with the Flink output.

Reads the benchmark slice directly from CSV (bypassing Kafka and Flink), computes EMA-38/EMA-100
per symbol over 5-minute tumbling windows and the BUY/SELL crossovers, then compares them with
the TimescaleDB exports written by `benchmark.py --dump`.

    python3 bench/reference.py data/week-prices.csv week-p4 week-p8 weekprefix-p4
"""
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
WINDOW_MS = 5 * 60 * 1000
OUT_OF_ORDERNESS_MS = 5000
ZONE = ZoneInfo("Europe/Paris")
EXAMPLE_SYMBOLS = 3
PARTITIONS = 8


def murmur2(data):
    """Kafka's default partitioner hash, so events land in the same partitions as in the producer."""
    length, m = len(data), 0x5BD1E995
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = data[i] | data[i + 1] << 8 | data[i + 2] << 16 | data[i + 3] << 24
        k = (k * m) & 0xFFFFFFFF
        k = ((k ^ k >> 24) * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    rem, base = length % 4, length - length % 4
    if rem == 3:
        h ^= data[base + 2] << 16
    if rem >= 2:
        h ^= data[base + 1] << 8
    if rem >= 1:
        h = ((h ^ data[base]) * m) & 0xFFFFFFFF
    h = ((h ^ h >> 13) * m) & 0xFFFFFFFF
    return h ^ h >> 15


def partition(symbol, cache={}):
    if symbol not in cache:
        cache[symbol] = (murmur2(symbol.encode()) & 0x7FFFFFFF) % PARTITIONS
    return cache[symbol]


def midnight_ms(date, cache={}):
    if date not in cache:
        cache[date] = int(datetime.strptime(date, "%d-%m-%Y").replace(tzinfo=ZONE).timestamp() * 1000)
    return cache[date]


def time_ms(text):
    """Milliseconds since midnight, truncating sub-millisecond digits like Java's toEpochMilli."""
    hms, _, frac = text.partition(".")
    h, m, s = hms.split(":")
    if len(h) != 2 or len(m) != 2 or len(s) != 2 or not (frac.isdigit() or frac == ""):
        raise ValueError(text)
    return ((int(h) * 60 + int(m)) * 60 + int(s)) * 1000 + int((frac + "000")[:3])


def price_events(path):
    """Yields (symbol, event_ms, close_key, price) in file order, mirroring TrendJob's filter."""
    with open(path) as f:
        for line in f:
            if line.startswith(("#", "ID,")) or not line.strip():
                continue
            c = line.rstrip("\n").split(",")
            if not c[21] or float(c[21]) <= 0:
                continue
            date, clock = c[26] or c[2], c[23] or c[3]
            try:
                ms = midnight_ms(date) + time_ms(clock)
            except ValueError:
                continue
            yield c[0], ms, clock, float(c[21])


def reference(path):
    """Closes per (symbol, window) under batch semantics (every event counts) and streaming semantics.

    Streaming semantics drop an event whose window has already been evaluated. We model that with
    a watermark per Kafka partition in file order, which is an upper bound on Flink's: the window
    operator's watermark is the minimum over partitions and never exceeds any partition's own.
    """
    batch, streaming = {}, {}  # (symbol, window) -> (close_key, price)
    late, events = 0, 0
    partition_max = [None] * PARTITIONS
    late_from = {}  # symbol -> end (s) of the first window with a late event

    def offer(closes, symbol, window, key, price):
        prev = closes.get((symbol, window))
        if prev is None or key >= prev[0]:
            closes[(symbol, window)] = (key, price)
    for symbol, ms, key, price in price_events(path):
        events += 1
        window = ms // WINDOW_MS
        end_ms = (window + 1) * WINDOW_MS
        p = partition(symbol)
        is_late = partition_max[p] is not None and end_ms <= partition_max[p] - OUT_OF_ORDERNESS_MS
        if is_late:
            late += 1
            late_from[symbol] = min(late_from.get(symbol, end_ms // 1000), end_ms // 1000)
        partition_max[p] = ms if partition_max[p] is None else max(partition_max[p], ms)
        offer(batch, symbol, window, key, price)
        if not is_late:
            offer(streaming, symbol, window, key, price)
    # The job's final watermark is the slowest partition's, so later windows never fire
    final_ms = min(m for m in partition_max if m is not None)
    return {"events": events, "late_upper_bound": late, "late_from": late_from, "final_ms": final_ms,
            "symbols": len({s for s, _ in batch}), "batch": queries(batch), "streaming": queries(streaming)}


def queries(closes):
    """Query 1 (EMAs per window) and Query 2 (crossovers) from the closes of each window."""
    per_symbol = defaultdict(list)
    for (symbol, window), (_, price) in closes.items():
        per_symbol[symbol].append((window, price))
    a38, a100 = 2 / 39, 2 / 101
    emas, crossovers = {}, {}
    for symbol, windows in per_symbol.items():
        e38 = e100 = 0.0
        prev = None
        for window, price in sorted(windows):
            e38, e100 = price * a38 + e38 * (1 - a38), price * a100 + e100 * (1 - a100)
            end_s = (window + 1) * WINDOW_MS // 1000
            emas[(symbol, end_s)] = (price, e38, e100)
            if prev and min(prev) > 0 and e38 > 0 and e100 > 0:
                if prev[0] <= prev[1] and e38 > e100:
                    crossovers[(symbol, end_s)] = "BUY"
                elif prev[0] >= prev[1] and e38 < e100:
                    crossovers[(symbol, end_s)] = "SELL"
            prev = (e38, e100)
    return {"emas": emas, "crossovers": crossovers}


def load_dump(label):
    base = ROOT / "data/results"
    with open(base / f"{label}-ema_results.csv") as f:
        emas = {(r[0], int(r[1])): tuple(map(float, r[2:])) for r in csv.reader(f) if r}
    with open(base / f"{label}-crossover_events.csv") as f:
        crossovers = {(r[0], int(r[1])): r[2] for r in csv.reader(f) if r}
    return emas, crossovers


def compare(ref, semantics, label):
    emas, crossovers = load_dump(label)
    # Flink only fires windows the final watermark has passed
    cutoff = (ref["final_ms"] - OUT_OF_ORDERNESS_MS) // 1000
    ref_rows = {k: v for k, v in ref[semantics]["emas"].items() if k[1] <= cutoff}
    got_rows = {k: v for k, v in emas.items() if k[1] <= cutoff}
    both = ref_rows.keys() & got_rows.keys()
    error = {k: max(abs(a - b) / max(abs(a), 1e-12) for a, b in zip(ref_rows[k], got_rows[k])) for k in both}
    errors = list(error.values())
    ref_x = {k: v for k, v in ref[semantics]["crossovers"].items() if k[1] <= cutoff}
    got_x = {k: v for k, v in crossovers.items() if k[1] <= cutoff}
    # Differences are explained if they follow a possibly dropped late event of the same symbol,
    # since a changed close also shifts every later EMA of that symbol
    bad_rows = (ref_rows.keys() ^ got_rows.keys()) | {k for k, e in error.items() if e > 1e-9}
    bad_x = {k for k in ref_x.keys() | got_x.keys() if ref_x.get(k) != got_x.get(k)}

    def unexplained(keys):
        return sum(1 for s, end in keys if not (s in ref["late_from"] and end >= ref["late_from"][s]))
    return {
        "label": label, "semantics": semantics,
        "ema_rows_reference": len(ref_rows), "ema_rows_flink": len(got_rows), "ema_rows_matched": len(both),
        "ema_rows_missing": len(ref_rows.keys() - got_rows.keys()),
        "ema_rows_extra": len(got_rows.keys() - ref_rows.keys()),
        "ema_max_relative_error": max(errors, default=0.0),
        "ema_rows_equal": sum(1 for e in errors if e <= 1e-9),
        "ema_rows_different": len(bad_rows), "ema_rows_unexplained": unexplained(bad_rows),
        "symbols_different": len({s for s, _ in bad_rows}),
        "crossovers_different": len(bad_x), "crossovers_unexplained": unexplained(bad_x),
        "crossovers_reference": len(ref_x), "crossovers_flink": len(got_x),
        "crossovers_matched": sum(1 for k, v in got_x.items() if ref_x.get(k) == v),
        "crossovers_buy_reference": sum(v == "BUY" for v in ref_x.values()),
        "crossovers_sell_reference": sum(v == "SELL" for v in ref_x.values()),
    }


def examples(ref, label):
    """Series for the most-crossed liquid symbols, for the report's EMA figure."""
    emas, crossovers = load_dump(label)
    counts = defaultdict(int)
    for symbol, _ in crossovers:
        counts[symbol] += 1
    windows = defaultdict(int)
    for symbol, _ in emas:
        windows[symbol] += 1
    top = max(windows.values())
    picks = sorted((s for s in counts if windows[s] >= 0.9 * top), key=lambda s: (-counts[s], s))[:EXAMPLE_SYMBOLS]
    return {s: {"emas": sorted([k[1], *v] for k, v in emas.items() if k[0] == s),
                "crossovers": sorted([k[1], v] for k, v in crossovers.items() if k[0] == s)} for s in picks}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("slice")
    parser.add_argument("labels", nargs="+", help="dump labels from benchmark.py --dump")
    args = parser.parse_args()
    ref = reference(args.slice)
    out = ROOT / "report/results"
    result = {
        "slice": Path(args.slice).name, "price_events": ref["events"], "symbols": ref["symbols"],
        "late_upper_bound": ref["late_upper_bound"], "late_symbols": len(ref["late_from"]),
        "comparisons": [compare(ref, sem, label) for label in args.labels for sem in ("streaming", "batch")],
    }
    stem = Path(args.slice).stem
    (out / f"correctness-{stem}.json").write_text(json.dumps(result, indent=2))
    (out / f"examples-{stem}.json").write_text(json.dumps(examples(ref, args.labels[0])))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
