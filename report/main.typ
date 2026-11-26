#import "@preview/faithful-acmart:0.2.0": *

// Every number below is read from bench/ output, so rerunning the benchmark updates the report
#let summary = json("results/summary.json")
#let correctness = json("results/correctness-week-prices.json")
#let monday = json("results/correctness-bench-20000000.json")
#let unaligned = json("results/unaligned-week.json")
#let unaligned-tp = json("results/unaligned-throughput.json")
#let producer = json("results/producer-bench-20000000.json")
#let week = json("results/producer-week-prices.json")
#let runs(label) = summary.filter(r => r.label == label)
#let main-runs = runs("main")
#let prefix-runs = runs("prefix")
#let at(rows, p) = rows.find(r => r.parallelism == p)
#let ps = main-runs.map(r => r.parallelism)
#let pmax = calc.max(..ps)
#let num(x, digits: 0) = {
  let s = str(calc.round(x, digits: digits))
  if digits > 0 and not s.contains(".") { s += "." + "0" * digits }
  s
}
#let kev(x) = num(x / 1000) + "k"
#let mev(x, digits: 1) = num(x / 1e6, digits: digits) + "M"
#let pct(x, digits: 1) = num(100 * x, digits: digits) + "%"
#let tp(p) = at(main-runs, p).throughput_mean
#let cmp(result, label, semantics: "streaming") = result.comparisons.find(c => c.label == label and c.semantics == semantics)
#let fixed4 = cmp(correctness, "week-p4")
#let fixed8 = cmp(correctness, "week-p8")
#let broken = cmp(correctness, "weekprefix-p4")
#let late-of(label, p) = at(runs(label), p).late_dropped
#let identical(c) = c.ema_rows_different == 0 and c.crossovers_matched == c.crossovers_reference

#show: acmart.with(
  format: "sigconf",
  title: "Detecting Trading Trends in Financial Tick Data",
  subtitle: "CS-E4780 Scalable Systems and Data Management Course Project",
  conference: (
    name: "CS-E4780 Scalable Systems and Data Management",
    short: "CS-E4780",
    date: "November 2026",
    venue: "Espoo, Finland",
  ),
  copyright: "none",
  doi: none,
  isbn: none,
  print-acm-reference: false,
  acm-year: 2026,
  authors: (
    (
      name: "Joona Kärkkäinen",
      affiliation: (
        institution: "Aalto University",
        city: "Espoo",
        country: "Finland",
      ),
    ),
  ),
  abstract: [
    We present a stream processing system that computes exponential moving averages (EMAs) over
    5-minute tumbling windows for every symbol in the DEBS 2022 trading data set and turns their
    crossovers into buy and sell advisories. Events are replayed into Apache Kafka, processed by an
    Apache Flink job, stored in TimescaleDB and shown in a web trading terminal, while Prometheus
    collects metrics from Flink and from every container. On a single 8-core laptop the job
    processes #kev(tp(1)) events/s at parallelism 1 and #kev(tp(pmax)) events/s at parallelism
    #pmax. Comparing the output with an independent reference implementation exposed a
    watermarking bug that silently discarded
    #pct(late-of("weekprefix", 4) / correctness.price_events, digits: 0) of a week's price events,
    and a rarer nondeterminism when the job reads a backlog. With per-partition watermarks and
    watermark alignment, the output for the whole week is identical to the reference at every
    parallelism we tested.
  ],
  keywords: ("stream processing", "complex event processing", "Apache Flink", "Apache Kafka"),
)

= Introduction

Market data terminals help traders spot investment opportunities in high-volume streams of
financial events. A common intraday strategy watches two EMAs of a symbol's price: when the
short-interval EMA overtakes the long-interval one, the price is starting to rise (a bullish
breakout, so buy); when it falls below, the price is starting to drop (a bearish breakout, so
sell). The advisory is only useful if it arrives while the price is still favorable, so the
computation has to keep up with the market in real time.

The project data set @debs2022data holds 289 million events recorded by Infront Financial
Technology from 8 to 14 November 2021 for 5504 equities and indices on the Paris, Amsterdam and
Frankfurt exchanges. The project defines two queries. *Query 1* computes, for each symbol $s$ and
5-minute tumbling window $w_i$ starting from midnight, $"EMA"^j_(s,w_i) = "Close"_(s,w_i) dot
2/(1+j) + "EMA"^j_(s,w_(i-1)) dot (1 - 2/(1+j))$ for $j in {38, 100}$, where the close is the last
price observed in the window and $"EMA"^j_(s,w_0) = 0$. *Query 2* emits a buy advisory when
$"EMA"^38$ rises above $"EMA"^100$ between two consecutive windows and a sell advisory when it
falls below. All symbols must be computed, while the user only needs updates for the symbols
they subscribe to.

Section 2 describes the architecture, Section 3 its implementation, Section 4 evaluates
correctness, scalability and resource use, and Section 5 concludes. The source code, the
benchmark harness and this report are in the project's GitHub repository.

= System Architecture

#figure(
  {
    set text(size: 7pt)
    let node(title, body) = box(
      width: 100%, inset: 4pt, radius: 2pt, stroke: 0.5pt + luma(110),
      [*#title* #h(1fr) #body],
    )
    let arrow(label) = align(center, text(fill: luma(90))[#sym.arrow.b #label])
    stack(
      spacing: 2.5pt,
      node("CSV replay producer", [Java, 1 process]),
      arrow[JSON events keyed by symbol],
      node("Apache Kafka", [topic `ticks`, 8 partitions]),
      arrow[per-partition watermarks],
      box(width: 100%, inset: 4pt, radius: 2pt, stroke: 0.5pt + luma(110), stack(
        spacing: 2.5pt,
        [*Apache Flink job* #h(1fr) parallelism $p$, $ceil(p\/2)$ TaskManagers],
        node([Source #sym.arrow.r filter price events], []),
        arrow[keyBy symbol],
        node([5-min tumbling window #sym.arrow.r EMA-38/100], [Query 1]),
        arrow[keyBy symbol],
        node("Crossover detector", [Query 2]),
      )),
      arrow[JDBC upserts],
      node("TimescaleDB", [`ema_results`, `crossover_events`]),
      arrow[REST API],
      node("Web terminal", [Javalin + React]),
      v(2pt),
      node("Prometheus + Grafana", [Flink reporter, cAdvisor, web]),
    )
  },
  kind: image,
  caption: [System architecture. Every component runs as a Docker Compose service.],
) <fig-arch>

@fig-arch shows the pipeline. We picked components that each own one concern and can be
scaled on their own.

*Kafka as the replay buffer.* A producer replays the CSV files into Kafka, which decouples the
data source from processing: the job can be restarted, rescaled or rerun from the earliest
offset without touching the producer, and the benchmark can preload events so it measures the
job alone. Events are keyed by symbol, so each symbol's events stay ordered within one
partition. The topic has 8 partitions, which caps the useful source parallelism at 8.

*Flink for windowing and pattern detection.* Both queries are keyed by symbol, which Flink's
`keyBy` spreads over parallel subtasks with each symbol's state on exactly one subtask. Query 1
maps directly to event-time tumbling windows. Windows are aligned to the epoch, and because the
CET offset of the data is a whole hour, they also start at midnight CET as required. A window
fires when the watermark passes its end, which happens once events of the next window arrive
plus a 5-second bound on out-of-orderness. Watermarks are tracked per Kafka partition, and
watermark alignment pauses any partition that runs more than a minute ahead of the slowest one
(@sec-correct explains both). The previous window's EMAs are kept in keyed
`ValueState`. Query 2 is a `KeyedProcessFunction` over the EMA stream that stores the previous
pair of EMAs and applies the crossover conditions. Windows without a price event produce no
result, so a symbol's EMA advances only when it trades.

*TimescaleDB for results.* Results are upserted into hypertables keyed by
`(symbol, window_end)`. Upserts make the sinks idempotent: Flink checkpoints state and Kafka
offsets every minute to a shared volume, and after a failure it replays from the last
checkpoint and rewrites the same rows, so the stored results are the same as for a failure-free run.

*Subscriptions in the client.* The job computes every symbol, as the specification requires.
The subscribed subset is the terminal's watchlist: the browser fetches updates only for the
watched symbols, plus a feed of the latest advisories across all symbols.

*Observability.* Flink's Prometheus reporter exposes operator and task metrics (records in and
out, late records dropped, busy and backpressured time), and cAdvisor exposes CPU and memory per
container. Prometheus discovers each TaskManager replica through DNS, so scaling the service
needs no configuration change.

= Implementation

The JVM modules are a Maven multi-module project in Java 21: a shared event schema, the
producer, the Flink job and the web backend. The stack runs on Docker Compose with Kafka 4.3.1
in KRaft mode, Flink 2.3.0, TimescaleDB 2.30.2 on PostgreSQL 18, Prometheus 3.8.0, Grafana
12.2.0 and cAdvisor 0.52.1. A one-shot container builds the job and submits it through Flink's
REST API on startup. A Nix flake pins the development tools.

*Producer.* The producer streams each CSV line, keeps the attributes the queries need (ID,
security type, date, time, last price, trading time and trading date) and sends them as JSON
with LZ4-compressed batching. It replays #mev(producer.events) events in
#num(producer.seconds) s (#kev(producer.events_per_s) events/s), so it is never the
bottleneck. An optional rate limit simulates live arrival.

*Flink job.* The source parses events and assigns event time from the trading date and time,
falling back to the system date and time when they are empty (the trading date is missing for
most events). It then drops events without a positive price. The window pre-reduces to the
latest event by trading time, so its state is one event per symbol and open window instead of
every event, and a `ProcessWindowFunction` turns that event into the window's EMAs.

*Web terminal.* A Javalin server exposes the results through a REST API and serves a React and
TypeScript frontend built with Vite. For the selected symbol it draws the price with both EMAs,
BUY and SELL markers at each crossover and the EMA spread below, next to the watchlist and a
live feed of advisories. Grafana shows the system metrics.

*Benchmark harness.* `bench/benchmark.py` preloads Kafka, scales the TaskManagers, submits the
job at a given parallelism, waits until it has read every event and its sinks have drained, and
saves the Prometheus series of the run. `bench/reference.py` recomputes both queries
single-threaded from the CSV, and `bench/plot.py` draws the figures in this report.

= Performance Evaluation

*Setup.* All experiments run on an Apple M1 laptop (8 cores, 16 GB) with the containers in an
OrbStack Linux VM limited to 8 CPUs and 8 GB of memory. Each TaskManager has 2 slots and 1 GB of
memory, and a job at parallelism $p$ runs on $ceil(p\/2)$ TaskManagers. Workloads are preloaded
into Kafka, so every run reads the same backlog as fast as it can.

- *Scalability workload:* the first #mev(producer.events, digits: 0) events of Monday (00:00 to
  11:56 CET), of which #mev(at(main-runs, 1).price_events, digits: 2) are price events. Each
  configuration runs #at(main-runs, 1).runs times.
- *Correctness workload:* the price events of the whole week
  (#mev(week.events, digits: 1) events, #correctness.symbols symbols). The Monday slice alone
  produces no advisories: both EMAs start from zero and EMA-100 converges slowly, so the first
  crossovers appear on the second trading day. Non-price events are filtered out before the
  windows and do not advance watermarks, so leaving them out changes no result.

Throughput is events read divided by the time from the first to the last scraped increase of
the source's record counter. Prometheus scrapes every 5 seconds, so durations are resolved to
#sym.plus.minus 5 s: about #pct(5 / (producer.events / tp(1)), digits: 0) at $p = 1$ and
#pct(5 / (producer.events / tp(pmax)), digits: 0) at $p = #pmax$, where all runs fall into the
same 5-second step.

== Correctness <sec-correct>

`bench/reference.py` is an independent implementation that reads the CSV directly, so it shares
no code, serialization or ordering with Kafka and Flink. It applies the same filter, event time
and window assignment, picks the close by latest trading time, iterates the EMA recursion and
applies the crossover conditions. It computes two variants. *Batch semantics* accept every
event. *Streaming semantics* follow the specification: a window is evaluated once the next
window starts, so an event that arrives after its window has been evaluated cannot change it.
The reference models arrival with a watermark per Kafka partition in file order; it assigns
symbols to partitions with Kafka's own hash function, which reproduces the measured partition
sizes exactly. We compare every window that the job's final watermark has closed.

On the Monday slice no event is more than 5 seconds out of order within its partition
(#monday.late_upper_bound such events), so both variants coincide, and the job's output is
identical to them at every parallelism:
#{
  let mains = monday.comparisons.filter(c => c.semantics == "streaming" and c.label.starts-with("main"))
  [#mains.map(c => c.label.split("-p").last()).join(", ", last: " and ")]
} with all #cmp(monday, "main-p1").ema_rows_reference windows equal.
#if not monday.comparisons.all(identical) [*(Warning: a Monday comparison differs.)*]
@tab-correct shows the week.

#figure(
  table(
    columns: (auto, 1fr, 1fr, 1fr, 1fr),
    align: (left, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([], [Reference], [No fix\ $p = 4$], [Fixed\ $p = 4$], [Fixed\ $p = 8$]),
    table.hline(stroke: 0.5pt),
    [Late events], [#correctness.late_upper_bound], [#mev(late-of("weekprefix", 4), digits: 1)],
      [#num(late-of("week", 4))], [#num(late-of("week", 8))],
    [EMA windows], [#fixed4.ema_rows_reference], [#broken.ema_rows_flink], [#fixed4.ema_rows_flink],
      [#fixed8.ema_rows_flink],
    [#h(0.6em) identical], [--], [#broken.ema_rows_equal], [#fixed4.ema_rows_equal],
      [#fixed8.ema_rows_equal],
    [Advisories], [#fixed4.crossovers_reference], [#broken.crossovers_flink],
      [#fixed4.crossovers_flink], [#fixed8.crossovers_flink],
    [#h(0.6em) identical], [--], [#broken.crossovers_matched], [#fixed4.crossovers_matched],
      [#fixed8.crossovers_matched],
    table.hline(),
  ),
  caption: [The week of price events compared with the streaming reference. "No fix" assigns
    watermarks after the source; "Fixed" is the final job. The reference column counts the
    events late within their partition.],
) <tab-correct>

*Watermarks per partition.* The first version failed badly. It dropped
#pct(at(prefix-runs, 1).late_dropped / at(prefix-runs, 1).price_events, digits: 0) of the
Monday price events as late at parallelism 1 and
#pct(at(prefix-runs, 4).late_dropped / at(prefix-runs, 4).price_events, digits: 0) at
parallelism 4, although none of them is late within its partition. The cause was where
watermarks were generated. The job attached its watermark strategy after the Kafka source, so
each source subtask tracked one watermark over all of its partitions. The Kafka consumer returns
records in batches per partition, so a batch from a partition that is ahead in event time pushes
the watermark past events that the other partitions have not delivered yet, and the window
operator discards them. How far partitions drift apart depends on how many each subtask reads,
which made the losses depend on parallelism. Moving the strategy into the source makes Kafka
track a watermark per partition and forward their minimum, so a partition that is ahead can no
longer overtake one that is behind.

*Watermark alignment.* The week contains one anomalous batch: on Wednesday at 18:56,
#correctness.late_upper_bound index events were republished with a trading time of 01:00 and no
trading date, which places them 18 hours in the past. Under streaming semantics they must be
dropped, because their windows were evaluated long before. Without alignment the job dropped
only #unaligned.runs.map(r => num(r.late_dropped)).join(" and ") of them at parallelism
#unaligned.runs.map(r => str(r.parallelism)).join(" and "). The subtasks read the backlog at
different speeds in event time, so the window operator's watermark, the minimum over all
subtasks, sometimes lagged hours behind and let some of the batch in. Because each EMA feeds
the next, every accepted event shifted all later EMAs of its symbol: results differed in
#unaligned.runs.map(r => str(r.symbols_different)).join(" and ") symbols and
#unaligned.runs.map(r => str(r.crossovers_different)).join(" and ") advisories, with different
results at each parallelism. Watermark alignment pauses partitions that run more than a minute
ahead, which bounds that lag. With it the job drops exactly the events the reference predicts,
and its output is identical to the streaming reference at both parallelisms. Alignment changes
throughput in both directions (@tab-align): at parallelism 1, one subtask reads all 8 partitions
and has to keep them in step, which costs
#pct(1 - tp(1) / at(unaligned-tp, 1).throughput_mean, digits: 0); at higher parallelism, each
subtask reads fewer partitions and the steadier watermark lets windows fire evenly.

#figure(
  table(
    columns: (auto, 1fr, 1fr, auto),
    align: (left, right, right, right),
    stroke: none,
    table.hline(),
    table.header([$p$], [Without alignment], [With alignment], [Change]),
    table.hline(stroke: 0.5pt),
    ..main-runs.map(r => (
      [#r.parallelism],
      [#kev(at(unaligned-tp, r.parallelism).throughput_mean) events/s],
      [#kev(r.throughput_mean) events/s],
      [#{
        let d = r.throughput_mean / at(unaligned-tp, r.parallelism).throughput_mean - 1
        if d >= 0 { "+" } else { sym.minus } + pct(calc.abs(d), digits: 0)
      }],
    )).flatten(),
    table.hline(),
  ),
  caption: [Mean throughput on the Monday slice with and without watermark alignment.],
) <tab-align>
#if not (identical(fixed4) and identical(fixed8)) [*(Warning: a week comparison differs.)*]

Batch semantics would put the republished events back into Wednesday's 01:00 windows. That
changes the later EMAs of #correctness.late_symbols symbols, so
#num(cmp(correctness, "week-p4", semantics: "batch").ema_rows_different) windows differ from
the job's output. A stream processor can only reach that result by retracting windows it has
already emitted, which the specification rules out. @fig-ema shows the results for the
symbols with the most advisories.

#figure(
  image("figures/ema.svg", width: 100%),
  caption: [Close price, EMA-38 and EMA-100 per window over the week, with the advisories Query 2
    emitted, for the frequently traded symbols with the most crossovers. Both EMAs start from zero.],
  placement: top,
  scope: "parent",
) <fig-ema>

== Scalability

#figure(
  image("figures/throughput.svg", width: 100%),
  caption: [Mean throughput over #at(main-runs, 1).runs runs, with the range as error bars.],
) <fig-throughput>

#figure(
  table(
    columns: (auto, 1fr, auto, 1fr, auto),
    align: (left, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([$p$], [Events/s], [Speedup], [Rows/s], [Skew]),
    table.hline(stroke: 0.5pt),
    ..main-runs.map(r => (
      [#r.parallelism],
      [#kev(r.throughput_mean) #sym.plus.minus #kev(r.throughput_stdev)],
      [#num(r.throughput_mean / tp(1), digits: 2)#sym.times],
      [#num(r.rows_per_s)],
      [#num(r.skew, digits: 2)],
    )).flatten(),
    table.hline(),
  ),
  caption: [Throughput (mean #sym.plus.minus standard deviation), speedup over $p = 1$, EMA rows written per
    second, and events read by the busiest source subtask relative to the mean.],
) <tab-scale>

#figure(
  image("figures/cpu.svg", width: 100%),
  caption: [Mean CPU use per container while the job reads its input. The VM has 8 cores.],
) <fig-cpu>

#figure(
  image("figures/timeseries.svg", width: 100%),
  caption: [Events read, EMA rows written and CPU use during one run at parallelism 4.],
) <fig-timeseries>

@fig-throughput and @tab-scale show that throughput grows with parallelism but sub-linearly:
#num(tp(2) / tp(1), digits: 2)#sym.times at $p = 2$ and #num(tp(pmax) / tp(1), digits: 2)#sym.times at
$p = #pmax$. The machine is not the limit. The containers use at most
#num(at(main-runs, pmax).cpu_total, digits: 1) of the 8 cores (@fig-cpu).

@fig-busy locates the bottleneck. At every parallelism the task that holds the windows and the
EMA sink is busy #pct(calc.min(..main-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0)
to #pct(calc.max(..main-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0) of the time,
while the source waits on it: it is backpressured
#pct(at(main-runs, 1).backpressured.Source, digits: 0) of the time at $p = 1$ and still
#pct(at(main-runs, pmax).backpressured.Source, digits: 0) at $p = #pmax$. Busy time includes waiting on
synchronous calls, and the sink writes every result as its own JDBC upsert and transaction, so
the task mostly waits on database round trips rather than computing. @fig-timeseries shows the
consequence within one run: events read per second vary five-fold while rows written per
second vary far less. Events per second are therefore set by how many results each event produces.
At the start of the run, sparse pre-market updates produce a window result for almost every
event and the rate is lowest; during trading hours many events fall into the same window and
the rate peaks.

Parallelism adds concurrent sink connections, but rows per second grow from
#num(at(main-runs, 1).rows_per_s) at $p = 1$ to only #num(at(main-runs, pmax).rows_per_s) at
$p = #pmax$, as more writers share one TimescaleDB. Partition skew adds to this. With symbols as
keys, the 8 partitions are uneven, and at $p = #pmax$, where each subtask reads one partition,
the busiest subtask reads #num(at(main-runs, pmax).skew, digits: 2)#sym.times the mean. That alone caps
the speedup at #num(pmax / at(main-runs, pmax).skew, digits: 1)#sym.times and produces the tail in
@fig-timeseries, where subtasks that finished early leave the others reading alone.

#figure(
  image("figures/busy.svg", width: 100%),
  caption: [Share of time each task is busy or backpressured, averaged over its subtasks.],
) <fig-busy>

== Resource Utilization

@fig-cpu splits the CPU use by container. At $p = #pmax$ the TaskManagers use
#num(at(main-runs, pmax).cpu.taskmanagers, digits: 1) cores, TimescaleDB
#num(at(main-runs, pmax).cpu.timescaledb, digits: 1), Kafka
#num(at(main-runs, pmax).cpu.kafka, digits: 2) and the JobManager
#num(at(main-runs, pmax).cpu.jobmanager, digits: 2). Kafka is nearly idle because it serves the
preloaded, compressed batches from the page cache without decoding them. TimescaleDB's share
grows with the rate of upserts. Even at $p = #pmax$,
#pct(1 - at(main-runs, pmax).cpu_total / 8, digits: 0) of the VM's CPU capacity is idle.

Memory and state size mattered more than CPU, and two problems only appeared on long runs.
First, the original window buffered every event of an open window, and with per-partition
watermarks a window stays open until the slowest partition passes its end, so the 1 GB
TaskManagers ran out of heap. Pre-reducing each window to its latest event bounds the state by
the number of symbols times open windows. Flink also reserved 40% of TaskManager memory as
managed memory, which the heap-based state backend never uses; lowering that share to 10%
doubled the heap to about 340 MB. Second, without configured checkpoint storage Flink keeps
checkpoints in JobManager memory, capped at 5 MB per task. At parallelism 1 the state of all
symbols outgrew that after about two days of data, every checkpoint failed, and the job kept
restarting from its last good checkpoint without making progress. Checkpoints now go to a
volume shared by the JobManager and all TaskManagers.
#if main-runs.all(r => r.restarts == 0) [None of the scalability runs restarted.]

= Conclusion

The system computes both queries for all symbols and delivers advisories to a terminal that shows
them in context. Its results match an independent reference exactly at every parallelism we tested, and on
one laptop it processes #kev(tp(1)) to #kev(tp(pmax)) events/s depending on parallelism. The
most valuable part of the evaluation was the reference comparison: the first version looked
plausible on the dashboard while discarding most of its input, and a later version still gave
slightly different results at each parallelism. Only independently computed results revealed
either. The measurements point to the next improvements. The sink waits on one database
transaction per result, so batching upserts per checkpoint or writing asynchronously should
raise throughput the most, and more Kafka partitions than subtasks would smooth the skew of
symbol keys.

#bibliography("refs.bib")
