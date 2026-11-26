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

@fig-arch shows the pipeline. #todo[Overview and design rationale of the Flink job, its state,
subscriptions and the web terminal (team).]

*Kafka as the replay buffer.* The producer replays the CSV files into a Kafka topic with 8
partitions, keyed by symbol so that each symbol's events stay in order. Kafka decouples the data
source from processing: the job can be restarted, rescaled or rerun from the earliest offset,
and the benchmarks preload events so that they measure the job alone.

*Event time and watermarks.* Windows fire on event time once the watermark passes their end,
with a 5-second bound on out-of-orderness. Watermarks are tracked per Kafka partition, and
watermark alignment pauses partitions that run more than a minute ahead of the slowest one.
@sec-correct shows why both are needed.

*Storage and fault tolerance.* Results are upserted into TimescaleDB, so rewriting a row after
a failure is harmless. Flink checkpoints its state and Kafka offsets every minute to a shared
volume and replays from the last checkpoint after a failure. The EMA sink commits in batches:
when 1000 rows are queued, when a watermark passes (right after windows fire) and at every
checkpoint. Advisories are written one at a time so that they reach the trader immediately.
Prometheus collects Flink's task metrics and per-container CPU and memory from cAdvisor.

= Implementation

#todo[Module layout, stack and versions, windowing, EMA state, crossover detection and the web
terminal (team).]

*Producer.* The producer is a Java program that streams the CSV files, keeps the seven
attributes the queries need and sends them to Kafka as JSON, leaving out empty fields. Symbols
already include their exchange code, so they need no normalization and serve directly as Kafka
keys. It sends #kev(day.events_per_s) events/s, and an optional rate limit replays at a fixed
rate for the latency measurements.

*Event handling.* Most events are housekeeping or quote updates without a price. Because empty
fields are left out, the Flink source recognizes them by the missing price field and skips them
without parsing. It drops price events with a non-positive price or an invalid time and takes
event time from the trading date and time, or from the system date and time when those are
empty. Each window keeps only its latest event per symbol, so the state stays small.

*Benchmarks.* Scripts in `bench/` run the job at each parallelism and collect its metrics,
measure latency at fixed input rates, describe the data set, and recompute both queries with an
independent reference implementation. Every number in Section 4 comes from their output.

= Performance Evaluation

We ran all experiments on an Apple M1 laptop (four performance and four efficiency cores, 16 GB)
with the containers in a Linux VM limited to 8 CPUs and 8 GB of memory. Each TaskManager has
2 slots, so a job with parallelism $p$ runs on $ceil(p\/2)$ TaskManagers.

*Workloads.* In the DEBS 2022 data set @debs2022data, Monday's #mev(mon.events) events cover #mon.symbols symbols on all three exchanges
(#pct(mon.exchange_share.FR, digits: 0) Paris, #pct(mon.exchange_share.NL, digits: 0) Amsterdam,
#pct(mon.exchange_share.ETR, digits: 0) Frankfurt), but only
#pct(at(main-runs, 1).price_events / day.events, digits: 0) are price events, and
#pct(wk.type_share.I, digits: 0) of the week's price events come from indices. The load is
bursty and skewed: it averages #num(mon.mean_events_per_s) events/s with a peak of
#kev(mon.peak_events_per_s) events/s, and the busiest 1% of symbols produce
#pct(mon.top1pct_symbols_share, digits: 0) of the events. We measured throughput on all of
Monday, compared versions of the job and measured latency on its first
#mev(producer.events, digits: 0) events, and checked correctness on the week's
#mev(week.events) price events, since the first day produces no crossovers while EMA-100 warms
up. Throughput runs read a preloaded backlog; throughput is events read divided by the reading
time, measured to #sym.plus.minus 1 s from metrics scraped every second.

== Correctness <sec-correct>

We compared the job's output with an independent single-threaded implementation that reads the
CSV directly. It follows the specification's streaming semantics, where a window is evaluated
once the next one starts and later events cannot change it, and models a watermark per Kafka
partition. On the Monday slice the job's output is identical at parallelism
#{
  let mains = monday.comparisons.filter(c => c.semantics == "streaming" and c.label.starts-with("tuned"))
  [#mains.map(c => c.label.split("-p").last()).join(", ", last: " and ")]
}.
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
  caption: [The week of price events compared with the reference. "No fix" generates
    watermarks after the source.],
) <tab-correct>
#if not (identical(fixed4) and identical(fixed8)) [*(Warning: a week comparison differs.)*]

The comparison found two bugs. An early version generated one watermark per source subtask
over all its partitions, so a partition that was ahead in event time pushed the watermark past
events the others had not delivered yet. It dropped
#pct(at(prefix-runs, 1).late_dropped / at(prefix-runs, 1).price_events, digits: 0) of Monday's
price events at parallelism 1 and
#pct(late-of("weekprefix", 4) / correctness.price_events, digits: 0) of the week's at
parallelism 4. Generating watermarks per partition fixed this. The second bug was rarer: on
Wednesday at 18:56, #correctness.late_upper_bound events were republished with a time 18 hours
in the past. The specification requires dropping them, but subtasks reading the backlog at
different speeds let the watermark lag hours behind and accepted some of them, which changed
the results of #unaligned.runs.map(r => str(r.symbols_different)).join(" and ") symbols at
parallelism #unaligned.runs.map(r => str(r.parallelism)).join(" and "). Watermark alignment
bounds this lag. Measured before the optimizations below, it cost
#pct(1 - base-tp(1) / at(unaligned-tp, 1).throughput_mean, digits: 0) of throughput at
parallelism 1 and gained
#pct(base-tp(pmax) / at(unaligned-tp, pmax).throughput_mean - 1, digits: 0) at
parallelism #pmax.

== Throughput and Scalability <sec-opt>

#figure(
  table(
    columns: (auto, 1fr, 1fr, 1.3fr, auto, auto),
    align: (left, right, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([$p$], [First\ version], [Final], [Final,\ all Monday], [CPU\ cores], [Skew]),
    table.hline(stroke: 0.5pt),
    ..main-runs.map(r => (
      [#r.parallelism],
      [#kev(base-tp(r.parallelism))],
      [#kev(slice-tp(r.parallelism))],
      [#kev(r.throughput_mean) #sym.plus.minus #kev(r.throughput_stdev)],
      [#num(r.cpu_total, digits: 1)],
      [#num(r.skew, digits: 2)],
    )).flatten(),
    table.hline(),
  ),
  caption: [Mean throughput in events/s over #at(main-runs, 1).runs runs, on the first
    #mev(producer.events, digits: 0) Monday events unless noted. CPU cores and skew (events read
    by the busiest source subtask relative to the mean) are for the final job on all of Monday.],
) <tab-scale>

Flink's task metrics showed where the first version spent its time. The window and EMA sink
task was busy
#pct(calc.min(..base-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0) to
#pct(calc.max(..base-runs.map(r => r.busy.TumblingEventTimeWindows)), digits: 0) of the time
while the source waited on it, because the sink committed each result in its own transaction and
wrote only #num(at(base-runs, 1).rows_per_s) rows/s. Batching the commits raised throughput at
parallelism 1 from #kev(base-tp(1)) to #kev(batched-tp) events/s in a single run. The source
then became the bottleneck: it parsed every event although most carry no price, and parsed each
timestamp three times. Skipping non-price events before parsing and parsing timestamps once
brought parallelism 1 to #kev(slice-tp(1)) events/s with identical results (@tab-scale).

On all of Monday the final job reads #kev(tp(1)) events/s at parallelism 1,
#num(tp(1) / mon.peak_events_per_s) times the day's peak rate, and #kev(tp(4)) at parallelism 4
(#num(tp(4) / tp(1), digits: 1)#sym.times), but no more at parallelism #pmax. There no task is
saturated: the containers use #num(at(main-runs, pmax).cpu_total, digits: 1) of the 8 cores
and the source is busy #pct(at(main-runs, pmax).busy.Source, digits: 0) of the time but hardly
backpressured. Three effects, which we did not separate, leave the sources idle: watermark
alignment pauses partitions #pct(at(main-runs, pmax).paused, digits: 0) of the time, the long
tail of symbols makes the busiest partition #num(at(main-runs, pmax).skew, digits: 2)#sym.times
the mean, and beyond four busy subtasks some run on the M1's slower efficiency cores.

== Latency

#figure(
  table(
    columns: (1fr, 1fr, 1fr, 1fr),
    align: (left, right, right, right),
    stroke: none,
    table.hline(),
    table.header([Events/s], [Median], [p99], [Max]),
    table.hline(stroke: 0.5pt),
    ..latency.map(r => (
      [#kev(r.achieved_rate)],
      [#num(r.window_latency_ms.p50) ms],
      [#num(r.window_latency_ms.p99) ms],
      [#num(r.window_latency_ms.max) ms],
    )).flatten(),
    table.hline(),
  ),
  caption: [Time from when a window can be evaluated until all its EMA rows are committed, at
    parallelism #latency.first().parallelism and a steady input rate.],
) <tab-latency>

#let lat-steady = latency.slice(1)
To measure latency, the producer replays at a fixed rate into a running job and logs when it
sends the first price event of each window, the moment the previous window can be evaluated.
Latency is the time from then until the window's last EMA row is committed, so it includes the
5-second out-of-orderness bound and the batched commit. From #kev(lat-steady.first().rate) to
#kev(lat-steady.last().achieved_rate) events/s the median stays at or below
#num(calc.max(..lat-steady.map(r => r.window_latency_ms.p50))) ms and p99 at or below
#num(calc.max(..lat-steady.map(r => r.window_latency_ms.p99))) ms (@tab-latency); the
producer could not send faster while sharing the machine with the job. Latency is highest at
the lowest rate, where event time advances slowly and the watermark waits longer for sparse
partitions.

== Resource Utilization

At parallelism #pmax the TaskManagers use #num(at(main-runs, pmax).cpu.taskmanagers, digits: 1)
cores, TimescaleDB #num(at(main-runs, pmax).cpu.timescaledb, digits: 1) and Kafka almost none,
since it serves the preloaded batches from memory. No component uses the GPU. Memory caused
more trouble than CPU: the original windows buffered every event and ran the 1 GB TaskManagers
out of heap, and Flink's default checkpoint storage, capped at 5 MB per task, overflowed after
two days of data at parallelism 1 and left the job in a restart loop. Pre-reducing windows, a
smaller managed memory share and checkpoints on a shared volume fixed both.
#if main-runs.all(r => r.restarts == 0) [None of the scalability runs restarted.]

= Conclusion

#todo[Conclusion (team).]

*Future work.* The job stops scaling beyond four subtasks on this laptop although no task is
saturated. More Kafka partitions than subtasks would even out the skewed symbol keys, and
identical cores, for example in a cloud cluster, would show how much of the limit comes from
watermark alignment. Producing from several processes would let us measure latency closer to
the job's capacity.

= Contributions

Joona built the producer, the metrics setup, the benchmarks and reference implementation, the
watermark and checkpoint fixes and the performance optimizations. Aleksi set up Kafka and Flink
and built the EMA job, Query 2 and its persistence, job submission and the trading terminal.
Axel built the first web frontend, the database schema and the Prometheus and Grafana setup.

*Joona Kärkkäinen.* #todo[Joona's answers.]

*Aleksi Kuusinen.* #todo[Aleksi's answers.]

*Axel Andersson.* #todo[Axel's answers.]

#bibliography("refs.bib")
