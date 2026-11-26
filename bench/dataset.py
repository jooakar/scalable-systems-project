#!/usr/bin/env python3
"""Describes the workloads for the report: exchange and security-type shares, arrival rates and
how events spread over symbols. Writes report/results/dataset.json.

    python3 bench/dataset.py data/debs2022-gc-trading-day-08-11-21.csv data/week-prices.csv
"""
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent / "report/results"

# awk is several times faster than Python over the multi-GB files; it emits one count per line
AWK = r'''$1 !~ /^#/ && $1 != "ID" {
  sym[$1]++; type[$2]++; sec[$3 " " substr($4, 1, 8)]++; day[$3]++
  n = split($1, parts, "."); exchange[parts[n]]++
}
END {
  for (k in sym) print "symbol\t" k "\t" sym[k]
  for (k in type) print "type\t" k "\t" type[k]
  for (k in exchange) print "exchange\t" k "\t" exchange[k]
  for (k in sec) print "second\t" k "\t" sec[k]
  for (k in day) print "day\t" k "\t" day[k]
}'''


def describe(path):
    out = subprocess.run(["awk", "-F,", AWK, path], check=True, text=True, capture_output=True,
                         env={"LC_ALL": "C"}).stdout
    counts = {"symbol": Counter(), "type": Counter(), "exchange": Counter(), "second": Counter(), "day": Counter()}
    for line in out.splitlines():
        kind, key, count = line.split("\t")
        counts[kind][key] = int(count)
    total = sum(counts["symbol"].values())
    per_symbol = sorted(counts["symbol"].values(), reverse=True)

    def top_share(fraction):
        return sum(per_symbol[:max(1, round(len(per_symbol) * fraction))]) / total
    peak_second, peak = counts["second"].most_common(1)[0]
    # A few events carry a neighbouring day's date; only days with real traffic count
    days = sum(1 for n in counts["day"].values() if n >= 0.01 * total)
    return {
        "events": total, "symbols": len(per_symbol),
        "exchange_share": {k: v / total for k, v in counts["exchange"].items()},
        "type_share": {k: v / total for k, v in counts["type"].items()},
        "top1pct_symbols_share": top_share(0.01), "top10pct_symbols_share": top_share(0.1),
        "median_events_per_symbol": per_symbol[len(per_symbol) // 2],
        "peak_events_per_s": peak, "peak_second": peak_second, "days": days,
        "mean_events_per_s": total / (86400 * days),
    }


def main():
    result = {Path(p).stem: describe(p) for p in sys.argv[1:]}
    (RESULTS / "dataset.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
