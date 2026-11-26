#!/usr/bin/env python3
"""Turns report/results into the report's figures (report/figures/*.svg) and summary table."""
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("svg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "report/results"
FIGURES = ROOT / "report/figures"

# Reference light palette (dataviz skill), assigned in fixed slot order
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#ffffff"
# Task names depend on operator chaining, so match them by their first operator
TASKS = ["Source", "TumblingEventTimeWindows", "KeyedProcess"]
GROUPS = ["taskmanagers", "kafka", "timescaledb", "jobmanager"]

# Same typeface as the report body; SVG text stays text and Typst renders it with these fonts
for font in (ROOT / "report/fonts").glob("LibertinusSerif-*.otf"):
    font_manager.fontManager.addfont(font)
plt.rcParams.update({
    "font.family": "Libertinus Serif", "font.size": 8, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "legend.frameon": False, "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "lines.linewidth": 1.5, "svg.fonttype": "none",
})


def only(series):
    return next(iter(series.values()), [])


def window(run):
    """Interval in which the source was reading: first sample above zero to first sample at N."""
    read = only(run["series"]["records_read"])
    start = next(t for t, v in read if v > 0)
    end = next(t for t, v in read if v >= run["events"])
    return start, end


def mean_in(series, start, end):
    values = [v for t, v in series if start <= t <= end and v == v]  # v != v filters NaN
    return statistics.fmean(values) if values else 0.0


def summarize(run):
    start, end = window(run)
    s = run["series"]
    def per_task(key):
        return {task: mean_in(v, start, end) / 1000 for k, v in s[key].items()
                for task in TASKS if json.loads(k).get("task_name", "").startswith(task)}
    busy, bp = per_task("busy_ms_per_s"), per_task("backpressured_ms_per_s")
    return {
        "parallelism": run["parallelism"], "rep": run["rep"],
        "throughput": run["events"] / (end - start),
        "rows_per_s": run["ema_rows"] / (end - start),
        "skew": max(run["read_per_subtask"]) / statistics.fmean(run["read_per_subtask"]),
        "cpu": {g: mean_in(only(s[f"cpu_cores:{g}"]), start, end) for g in GROUPS},
        "memory_peak": {g: max((v for t, v in only(s[f"memory_bytes:{g}"]) if start <= t <= end), default=0)
                        for g in GROUPS},
        "busy": busy, "backpressured": bp,
        "paused": mean_in(only(s["paused_ms_per_s"]), start, end) / 1000 if "paused_ms_per_s" in s else None,
        "late_dropped": run["late_dropped"], "price_events": run["price_events"], "restarts": run["restarts"],
        "ema_rows": run["ema_rows"], "crossover_rows": run["crossover_rows"],
    }


def by_parallelism(summaries):
    groups = defaultdict(list)
    for s in summaries:
        groups[s["parallelism"]].append(s)
    return dict(sorted(groups.items()))


def save(fig, name):
    FIGURES.mkdir(exist_ok=True)
    fig.savefig(FIGURES / f"{name}.svg", bbox_inches="tight")
    plt.close(fig)


def ema_figure(examples):
    zone = ZoneInfo("Europe/Paris")
    symbols = list(examples)
    fig, axes = plt.subplots(len(symbols), 1, figsize=(3.3, 1.15 * len(symbols)), sharex=True)
    for ax, symbol in zip(axes, symbols):
        rows = examples[symbol]["emas"]
        times = [datetime.fromtimestamp(r[0], zone).replace(tzinfo=None) for r in rows]
        ax.plot(times, [r[1] for r in rows], color=MUTED, linewidth=1, label="Close")
        ax.plot(times, [r[2] for r in rows], color=SERIES[0], label="EMA-38")
        ax.plot(times, [r[3] for r in rows], color=SERIES[1], label="EMA-100")
        prices = {r[0]: r[1] for r in rows}
        for kind, marker, color in [("BUY", "^", SERIES[2]), ("SELL", "v", SERIES[3])]:
            xs = [t for t, k in examples[symbol]["crossovers"] if k == kind]
            ax.scatter([datetime.fromtimestamp(t, zone).replace(tzinfo=None) for t in xs],
                       [prices[t] for t in xs], marker=marker, s=22, color=color, edgecolor=SURFACE,
                       linewidth=1, zorder=3, label=f"{kind} advisory")
        # Skip the warm-up where both EMAs climb from zero; it would flatten the interesting part
        settled = [r[1] for r in rows[len(rows) // 3:]] + [r[2] for r in rows[len(rows) // 3:]]
        pad = (max(settled) - min(settled)) * 0.3
        ax.set_ylim(min(settled) - pad, max(settled) + pad)
        ax.set_ylabel(symbol)
    axes[0].legend(loc="lower left", ncols=3, fontsize=6.5, bbox_to_anchor=(0, 1.02), handlelength=1.2,
                   columnspacing=0.8)
    axes[-1].xaxis.set_major_locator(matplotlib.dates.DayLocator())
    axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%a"))
    first, last = (datetime.fromtimestamp(examples[symbols[0]]["emas"][i][0], zone) for i in (0, -1))
    axes[-1].set_xlabel(f"Window end (CET), {first:%d} to {last:%d %B %Y}")
    save(fig, "ema")


def table(by_label):
    rows = []
    for label, gs in by_label.items():
        for p, runs in gs.items():
            rows.append({
                "label": label, "parallelism": p, "runs": len(runs),
                "throughput_mean": statistics.fmean(r["throughput"] for r in runs),
                "throughput_stdev": statistics.stdev(r["throughput"] for r in runs) if len(runs) > 1 else 0,
                "rows_per_s": statistics.fmean(r["rows_per_s"] for r in runs),
                "skew": statistics.fmean(r["skew"] for r in runs),
                "busy": {t: statistics.fmean(r["busy"].get(t, 0) for r in runs) for t in TASKS},
                "backpressured": {t: statistics.fmean(r["backpressured"].get(t, 0) for r in runs) for t in TASKS},
                "paused": None if any(r["paused"] is None for r in runs) else statistics.fmean(r["paused"] for r in runs),
                "cpu_total": statistics.fmean(sum(r["cpu"].values()) for r in runs),
                "cpu": {g: statistics.fmean(r["cpu"][g] for r in runs) for g in GROUPS},
                "memory_peak": {g: statistics.fmean(r["memory_peak"][g] for r in runs) for g in GROUPS},
                "restarts": sum(r["restarts"] for r in runs),
                "late_dropped": statistics.fmean(r["late_dropped"] for r in runs),
                "price_events": statistics.fmean(r["price_events"] for r in runs),
                "ema_rows": statistics.fmean(r["ema_rows"] for r in runs),
                "crossover_rows": statistics.fmean(r["crossover_rows"] for r in runs),
            })
    (RESULTS / "summary.json").write_text(json.dumps(rows, indent=2))
    for r in rows:
        print({k: round(v, 3) if isinstance(v, float) else v for k, v in r.items()})


def main():
    runs = [json.loads(p.read_text()) for p in sorted((RESULTS / "runs").glob("*.json"))]
    labels = sorted({r["label"] for r in runs})
    table({label: by_parallelism(summarize(r) for r in runs if r["label"] == label) for label in labels})
    ema_figure(json.loads((RESULTS / "examples-week-prices.json").read_text()))


if __name__ == "__main__":
    main()
