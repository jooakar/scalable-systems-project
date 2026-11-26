#import "@preview/faithful-acmart:0.2.0": *

// Every number below is read from bench/ output, so rerunning the benchmark updates the report
#let summary = json("results/summary.json")
#let correctness = json("results/correctness-week-prices.json")
#let monday = json("results/correctness-bench-20000000.json")
#let unaligned = json("results/unaligned-week.json")
#let unaligned-tp = json("results/unaligned-throughput.json")
#let producer = json("results/producer-bench-20000000.json")
#let day = json("results/producer-debs2022-gc-trading-day-08-11-21.json")
#let week = json("results/producer-week-prices.json")
#let dataset = json("results/dataset.json")
#let mon = dataset.at("debs2022-gc-trading-day-08-11-21")
#let wk = dataset.at("week-prices")
#let latency = json("results/latency.json")
#let runs(label) = summary.filter(r => r.label == label)
// "main" is the first version of the job, "batched" adds the batched sink, "tuned" is the
// final job, all on the 20M-event slice; "day" is the final job on all of Monday
#let base-runs = runs("main")
#let slice-runs = runs("tuned")
#let main-runs = runs("day")
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
#let base-tp(p) = at(base-runs, p).throughput_mean
#let slice-tp(p) = at(slice-runs, p).throughput_mean
#let batched-tp = at(runs("batched"), 1).throughput_mean
#let tp-max = calc.max(..main-runs.map(r => r.throughput_mean))
#let cmp(result, label, semantics: "streaming") = result.comparisons.find(c => c.label == label and c.semantics == semantics)
#let fixed4 = cmp(correctness, "weektuned-p4")
#let fixed8 = cmp(correctness, "weektuned-p8")
#let broken = cmp(correctness, "weekprefix-p4")
#let late-of(label, p) = at(runs(label), p).late_dropped
#let identical(c) = c.ema_rows_different == 0 and c.crossovers_matched == c.crossovers_reference
#let todo(body) = text(fill: red)[*TODO:* #body]

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
      affiliation: (institution: "Aalto University", city: "Espoo", country: "Finland"),
    ),
    (
      name: "Aleksi Kuusinen",
      affiliation: (institution: "Aalto University", city: "Espoo", country: "Finland"),
    ),
    (
      name: "Axel Andersson",
      affiliation: (institution: "Aalto University", city: "Espoo", country: "Finland"),
    ),
  ),
  abstract: [#todo[Abstract (team).]],
  keywords: ("stream processing", "complex event processing", "Apache Flink", "Apache Kafka"),
)

= Introduction

#todo[Introduction (team): background, the data set and the two queries.]

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
        node([Source: skip non-price events, assign event time], []),
        arrow[keyBy symbol],
        node([5-min tumbling window #sym.arrow.r EMA-38/100], [Query 1]),
        arrow[keyBy symbol],
        node("Crossover detector", [Query 2]),
      )),
      arrow[batched JDBC upserts],
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

@fig-arch shows the pipeline. #todo[Overview and design rationale of the Flink job (windows, EMA
and crossover state), subscriptions and the web terminal (team).]

*Kafka as the replay buffer.* A producer replays the CSV files into Kafka, which decouples the
data source from processing: the job can be restarted, rescaled or rerun from the earliest
offset without touching the producer, and the benchmark preloads events so it measures the job
alone. Events are keyed by symbol, so each symbol's events stay ordered within one partition.
The 8 partitions cap the useful source parallelism at 8.

*Event time and watermarks.* A window fires when the watermark passes its end, that is once
events of the next window arrive plus a 5-second bound on out-of-orderness. The Kafka source
tracks a watermark per partition and forwards their minimum, and watermark alignment pauses
any partition that runs more than a minute of event time ahead of the slowest one
(@sec-correct shows why both are needed). Only price events carry event time; the source
drops all others before they reach the watermark generator.

*Results and fault tolerance.* Results are upserted into TimescaleDB hypertables keyed by
`(symbol, window_end)`, which makes the sinks idempotent. Flink checkpoints state and Kafka
offsets every minute to a volume shared by all Flink containers, and after a failure it replays
from the last checkpoint and rewrites the same rows. The EMA sink commits its upserts in
batches, triggered three ways: 1000 queued rows amortize commits under load, a passing
watermark (right after windows fire) keeps the terminal current at low rates, and a checkpoint
never covers rows that are not committed. Advisories are rare and must reach the trader at
once, so they are written one at a time.

*Observability.* Flink's Prometheus reporter exposes operator and task metrics (records in and
out, late records dropped, busy, backpressured and alignment-paused time), and cAdvisor exposes
CPU and memory per container. Prometheus discovers each TaskManager replica through DNS, so
scaling the service needs no configuration change.

= Implementation

#todo[Module layout, stack and versions, the Flink job's windowing, EMA state and crossover
detection, and the web terminal (team).]

*Producer.* The producer streams each CSV line, keeps the attributes the queries need (ID,
security type, date, time, last price, trading time and trading date) and sends them to Kafka as
JSON with empty fields omitted, in LZ4-compressed batches. Symbols already carry their exchange
code (`ID.[Exchange]`), so they need no normalization and serve directly as Kafka keys. It
replays all #mev(day.events) events of Monday in #num(day.seconds) s
(#kev(day.events_per_s) events/s). A rate limit can replay at a fixed number of events per
second, and for latency measurements the producer logs when it sent the first price event of
each window.

*Event handling in the source.* Most events are housekeeping or quote updates without a price.
Since empty fields are omitted, the deserializer recognizes them by the missing `"last"` key and
skips them without parsing. It parses price events and drops those whose price is not positive
or whose time is missing or invalid. Event time comes from the trading date and time, falling
back to the system date and time when they are empty (the trading date is missing for most
events), and is parsed once per event. The window pre-reduces to the latest event by trading
time, so its state is one event per symbol and open window.

*Benchmark harness.* `bench/benchmark.py` preloads Kafka, scales the TaskManagers, runs the
job at a given parallelism until it has read every event and its sinks have drained, and saves
the run's Prometheus series. `bench/latency.py` measures result latency at fixed input rates,
`bench/reference.py` recomputes both queries single-threaded from the CSV, and
`bench/dataset.py` describes the workloads. Section 4 reads every number from their output.

= Performance Evaluation

*Setup.* All experiments run on an Apple M1 laptop (four performance and four efficiency cores,
16 GB) with the containers in an OrbStack Linux VM limited to 8 CPUs and 8 GB of memory. Each
TaskManager has 2 slots and 1 GB of memory, and a job at parallelism $p$ runs on $ceil(p\/2)$
TaskManagers.

*Workloads.* Monday has #mev(mon.events) events for #mon.symbols symbols, from all three
exchanges: #pct(mon.exchange_share.FR, digits: 0) Paris,
#pct(mon.exchange_share.NL, digits: 0) Amsterdam and #pct(mon.exchange_share.ETR, digits: 0)
Frankfurt. Only #pct(at(main-runs, 1).price_events / day.events, digits: 0) are price events,
and while indices are #pct(mon.type_share.I, digits: 0) of all events, they produce
#pct(wk.type_share.I, digits: 0) of the week's price events.
The load is bursty, averaging #num(mon.mean_events_per_s) events/s over the day with a peak of
#kev(mon.peak_events_per_s) events/s at #mon.peak_second.split(" ").last() CET, and long-tailed:
the busiest 1% of symbols produce #pct(mon.top1pct_symbols_share, digits: 0) of the events and
the busiest 10% produce #pct(mon.top10pct_symbols_share, digits: 0). We use all of Monday for scalability, its first
#mev(producer.events, digits: 0) events (00:00 to 11:56 CET) to compare versions of the job
and to measure latency, and the week's #mev(week.events) price events for correctness, since
the Monday slice produces no advisories (EMA-100 starts from zero and converges slowly).
Non-price events affect no result, so the week workload leaves them out.

Throughput runs read a preloaded backlog as fast as they can. Throughput is events read
divided by the time from the first to the last increase of the Kafka consumer's record counter,
which Prometheus scrapes every second, so durations are resolved to #sym.plus.minus 1 s, about
#pct(1 / (day.events / tp(pmax)), digits: 0) of the shortest scalability run.

== Correctness <sec-correct>

The reference implementation reads the CSV directly, so it shares no code, serialization or
ordering with Kafka and Flink, and computes both queries under two semantics. *Batch semantics*
accept every event. *Streaming semantics* follow the specification: a window is evaluated once
the next window starts, so a later event cannot change it. The reference models arrival with a
watermark per Kafka partition in file order, using Kafka's hash function to assign symbols, which
reproduces the measured partition sizes exactly.

On the Monday slice no event is more than 5 seconds out of order within its partition, so both
semantics coincide, and the job's output is identical to them at every parallelism:
#{
  let mains = monday.comparisons.filter(c => c.semantics == "streaming" and c.label.starts-with("tuned"))
  [#mains.map(c => c.label.split("-p").last()).join(", ", last: " and ")]
} with all #cmp(monday, "tuned-p1").ema_rows_reference windows equal.
#if not monday.comparisons.all(identical) [*(Warning: a Monday comparison differs.)*]
@tab-correct shows the week.

#figure(
  table(
    columns: (auto, 1fr, 1fr, 1fr, 1fr),
    align: (left, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([], [Reference], [No fix\ $p = 4$], [Final\ $p = 4$], [Final\ $p = 8$]),
    table.hline(stroke: 0.5pt),
    [Late events], [#correctness.late_upper_bound], [#mev(late-of("weekprefix", 4), digits: 1)],
      [#num(late-of("weektuned", 4))], [#num(late-of("weektuned", 8))],
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
    watermarks after the source. The reference column counts the events late within their
    partition.],
) <tab-correct>
#if not (identical(fixed4) and identical(fixed8)) [*(Warning: a week comparison differs.)*]

*Watermarks per partition.* An earlier version dropped
#pct(at(prefix-runs, 1).late_dropped / at(prefix-runs, 1).price_events, digits: 0) of the
Monday price events as late at parallelism 1 and
#pct(at(prefix-runs, 4).late_dropped / at(prefix-runs, 4).price_events, digits: 0) at
parallelism 4, although none is late within its partition. It generated watermarks after the
Kafka source, one per subtask over all its partitions. The consumer returns records in batches
per partition, so a batch from a partition ahead in event time pushed the watermark past events
that other partitions had not delivered yet, and how far they drifted apart depended on the
parallelism. Generating watermarks in the source tracks one per partition.

*Watermark alignment.* On Wednesday at 18:56, #correctness.late_upper_bound index events were
republished with a trading time of 01:00 and no trading date, 18 hours in the past. Streaming
semantics must drop them. Without alignment the job dropped only
#unaligned.runs.map(r => num(r.late_dropped)).join(" and ") of them at parallelism
#unaligned.runs.map(r => str(r.parallelism)).join(" and "): subtasks read the backlog at
different speeds, so the window operator's watermark sometimes lagged hours behind and let part
of the batch in. Each accepted event shifted all later EMAs of its symbol, so results differed in
#unaligned.runs.map(r => str(r.symbols_different)).join(" and ") symbols and
#unaligned.runs.map(r => str(r.crossovers_different)).join(" and ") advisories, differently at
each parallelism. Alignment bounds that lag, and with it the job drops exactly the events the
reference predicts. Measured on the first version of the job, it costs
#pct(1 - base-tp(1) / at(unaligned-tp, 1).throughput_mean, digits: 0) of throughput at
parallelism 1, where one subtask must keep all 8 partitions in step, and gains
#pct(base-tp(pmax) / at(unaligned-tp, pmax).throughput_mean - 1, digits: 0) at parallelism
#pmax, where the steadier watermark lets windows fire evenly.

Batch semantics would put the republished events back into their windows and change
#num(cmp(correctness, "weektuned-p4", semantics: "batch").ema_rows_different) later windows of
#correctness.late_symbols symbols, a result a stream processor can only reach by retracting
emitted windows. @fig-ema shows the output for the symbols with the most advisories.

#figure(
  image("figures/ema.svg", width: 100%),
  caption: [Close price, EMA-38 and EMA-100 per window over the week, with the advisories Query 2
    emitted, for the frequently traded symbols with the most crossovers. Both EMAs start from zero.],
) <fig-ema>

== Removing Bottlenecks <sec-opt>

#figure(
  table(
    columns: (auto, 1fr, 1fr, auto),
    align: (left, right, right, right),
    stroke: none,
    table.hline(),
    table.header([$p$], [First version], [Final job], [Speedup]),
    table.hline(stroke: 0.5pt),
    ..base-runs.map(r => (
      [#r.parallelism],
      [#kev(r.throughput_mean) events/s],
      [#kev(slice-tp(r.parallelism)) events/s],
      [#num(slice-tp(r.parallelism) / r.throughput_mean, digits: 1)#sym.times],
    )).flatten(),
    table.hline(),
  ),
  caption: [Mean throughput on the 20M-event slice before and after removing the bottlenecks.],
) <tab-opt>

The first version read #kev(base-tp(1)) events/s at $p = 1$ (@tab-opt). The task holding the
windows and the EMA sink was busy
#pct(calc.min(..base-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0) to
#pct(calc.max(..base-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0) of the time
at every parallelism, while the source was backpressured
#pct(at(base-runs, 1).backpressured.Source, digits: 0) of the time at $p = 1$. Busy time
includes waiting on synchronous calls, and the sink committed every result in its own
transaction, so the task mostly waited on database round trips and wrote only
#num(at(base-runs, 1).rows_per_s) rows/s. Batching the commits alone raised $p = 1$ to
#kev(batched-tp) events/s in a single run.

The source then became the busiest task
(#pct(at(runs("batched"), 1).busy.Source, digits: 0) busy). A microbenchmark of its per-event
work found two costs of similar size: parsing the JSON of every event, although most carry no
price, and converting the time of each price event, which the job did three times.
Skipping non-price events before parsing and parsing each time once roughly halved the cost per
event and brought $p = 1$ to #kev(slice-tp(1)) events/s, with output still identical to the
reference.

== Scalability

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
  caption: [Throughput on all of Monday over #at(main-runs, 1).runs runs (mean #sym.plus.minus
    standard deviation), speedup over $p = 1$, EMA rows written per second, and events read by
    the busiest source subtask relative to the mean.],
) <tab-scale>

#figure(
  image("figures/busy.svg", width: 100%),
  caption: [Share of time each task is busy or backpressured, averaged over its subtasks.],
) <fig-busy>

At $p = 1$ the job reads #kev(tp(1)) events/s, #num(tp(1) / mon.peak_events_per_s) times
Monday's peak rate. With more subtasks it grows to #kev(tp(4)) events/s at $p = 4$
(#num(tp(4) / tp(1), digits: 2)#sym.times) and no further at $p = #pmax$, where it also
varies most between runs (@tab-scale). No task is saturated at $p = #pmax$
(@fig-busy): the source is busy #pct(at(main-runs, pmax).busy.Source, digits: 0) and the window
task #pct(at(main-runs, pmax).busy.TumblingEventTimeWindows, digits: 0) of the time, the source
is backpressured only #pct(at(main-runs, pmax).backpressured.Source, digits: 0), and the
containers use #num(at(main-runs, pmax).cpu_total, digits: 1) of the 8 cores (@fig-cpu). The
sources are mostly idle, for three reasons we did not separate. Since the job reads a whole
trading day in under a minute, small differences in reading speed trigger watermark alignment:
partitions are paused #pct(at(main-runs, pmax).paused, digits: 0) of the time at $p = #pmax$,
where each subtask has only one partition to read. The long tail of symbols makes partitions
uneven: the busiest subtask reads #num(at(main-runs, pmax).skew, digits: 2)#sym.times the mean,
which alone caps the speedup at #num(pmax / at(main-runs, pmax).skew, digits: 1)#sym.times.
Beyond four busy subtasks, some run on the M1's slower efficiency cores, and the watermark makes
the job wait for the slowest subtask.

== Latency

#figure(
  table(
    columns: (auto, auto, 1fr, 1fr, 1fr),
    align: (left, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([Target], [Achieved], [Median], [p99], [Max]),
    table.hline(stroke: 0.5pt),
    ..latency.map(r => (
      [#kev(r.rate)/s],
      [#kev(r.achieved_rate)/s],
      [#num(r.window_latency_ms.p50) ms],
      [#num(r.window_latency_ms.p99) ms],
      [#num(r.window_latency_ms.max) ms],
    )).flatten(),
    table.hline(),
  ),
  caption: [Time from when a window can be evaluated until all its EMA rows are committed, at
    $p = #latency.first().parallelism$ under a steady input rate.],
) <tab-latency>

#let lat-steady = latency.slice(1)
#let lat-low = latency.first()
@tab-latency measures, for each window, the time from when it can first be evaluated (the
producer sends the first price event of the next window) until its last EMA row is committed.
This covers the out-of-orderness bound, the wait for the slowest partition, processing and the
batched commit; the database and host clocks agree to within
#num(calc.max(..latency.map(r => r.clock.round_trip_ms)) / 2) ms. From
#kev(lat-steady.first().rate) to #kev(lat-steady.last().achieved_rate) events/s the median
stays at or below #num(calc.max(..lat-steady.map(r => r.window_latency_ms.p50))) ms and p99 at
or below #num(calc.max(..lat-steady.map(r => r.window_latency_ms.p99))) ms, and the
producer, which shares the machine, cannot send faster. The lowest rate, run on the first
#mev(lat-low.events, digits: 0) events, has the highest tail
(#num(lat-low.window_latency_ms.p99) ms at p99): event time advances slowly in wall-clock time,
so the out-of-orderness bound and sparse partitions hold the watermark back longer. At the
data's real rates, event-time progress rather than load sets the latency. Advisories were not
measured separately; the crossover detector consumes the EMA stream and commits each advisory
on its own, so they trail their EMA rows by one operator and one insert.

== Resource Utilization

#figure(
  image("figures/cpu.svg", width: 100%),
  caption: [Mean CPU use per container while the job reads its input. The VM has 8 cores.],
) <fig-cpu>

At $p = #pmax$ the TaskManagers use #num(at(main-runs, pmax).cpu.taskmanagers, digits: 1)
cores, TimescaleDB #num(at(main-runs, pmax).cpu.timescaledb, digits: 1), Kafka
#num(at(main-runs, pmax).cpu.kafka, digits: 2) and the JobManager
#num(at(main-runs, pmax).cpu.jobmanager, digits: 2) (@fig-cpu). Kafka serves the preloaded,
compressed batches from the page cache without decoding them, and TimescaleDB's share grows
with the rate of upserts. The GPU is unused.

Memory, not CPU, caused the two problems that appeared only on long runs. The original window
buffered every event until the slowest partition passed its end, and the 1 GB
TaskManagers ran out of heap; pre-reducing windows bounds the state by symbols times open
windows, and lowering Flink's unused managed memory share from 40% to 10% doubled the heap to
about 340 MB. Flink's default checkpoint storage in JobManager memory is capped at 5 MB per task,
which the state of all symbols outgrew after two days of data at parallelism 1, so the job kept
restarting until checkpoints moved to the shared volume.
#if main-runs.all(r => r.restarts == 0) [None of the scalability runs restarted.]

= Conclusion

#todo[Conclusion (team).]

*Future work.* Beyond four subtasks the job stops scaling on this laptop although no task is
saturated. More Kafka partitions than subtasks would even out the skewed symbol keys, and a
cluster of identical cores would show how much of the limit is watermark alignment and how much
the hardware. The latency measurement stopped at the producer's rate; producing from several
processes would show latency near the job's capacity. Inside each batch the EMA sink still sends
one statement per row, and multi-row inserts or PostgreSQL's `COPY` would cut TimescaleDB's
CPU share, which grows with the result rate.

= Individual Contributions

From the Git history: Joona built the producer, the Flink metrics and cAdvisor setup, the
benchmark and reference implementation, the watermark and checkpoint fixes and the performance
optimizations. Aleksi set up Kafka and Flink in Docker Compose and built the EMA job, Query 2
and its persistence, automatic job submission and the trading terminal. Axel built the first
web frontend, the SQL schema and the Prometheus and Grafana setup. Each member answers three
questions: the most important design decisions, the most challenging parts, and their own
contribution and how the team worked.

*Joona Kärkkäinen.* #todo[Joona's answers.]

*Aleksi Kuusinen.* #todo[Aleksi's answers.]

*Axel Andersson.* #todo[Axel's answers.]

#bibliography("refs.bib")
