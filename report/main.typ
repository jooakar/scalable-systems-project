#import "@preview/faithful-acmart:0.2.0": *

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
keys. It sends 937k events/s, and an optional rate limit replays at a fixed
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

*Workloads.* In the DEBS 2022 data set @debs2022data, Monday's 54.0M events cover 5493 symbols on all three exchanges
(55% Paris, 27% Amsterdam, 18% Frankfurt), but only
22% are price events, and
82% of the week's price events come from indices. The load is
bursty and skewed: it averages 625 events/s with a peak of
23k events/s, and the busiest 1% of symbols produce
47% of the events. We measured throughput on all of
Monday, compared versions of the job and measured latency on its first
20M events, and checked correctness on the week's
59.7M price events, since the first day produces no crossovers while EMA-100 warms
up. Throughput runs read a preloaded backlog; throughput is events read divided by the reading
time, measured to #sym.plus.minus 1 s from metrics scraped every second.

== Correctness <sec-correct>

We compared the job's output with an independent single-threaded implementation that reads the
CSV directly. It follows the specification's streaming semantics, where a window is evaluated
once the next one starts and later events cannot change it, and models a watermark per Kafka
partition. On the Monday slice the job's output is identical at parallelism
1, 2, 4 and 8.
@tab-correct shows the week.

#figure(
  table(
    columns: (auto, 1fr, 1fr, 1fr, 1fr),
    align: (left, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([], [Reference], [No fix\ $p = 4$], [Final\ $p = 4$], [Final\ $p = 8$]),
    table.hline(stroke: 0.5pt),
    [Late events], [5506], [25.6M], [5506], [5506],
    [EMA windows], [1855465], [1133434], [1855465], [1855465],
    [#h(0.6em) identical], [--], [802372], [1855465], [1855465],
    [Advisories], [6379], [3279], [6379], [6379],
    [#h(0.6em) identical], [--], [2628], [6379], [6379],
    table.hline(),
  ),
  caption: [The week of price events compared with the reference. "No fix" generates
    watermarks after the source.],
) <tab-correct>

The comparison found two bugs. An early version generated one watermark per source subtask
over all its partitions, so a partition that was ahead in event time pushed the watermark past
events the others had not delivered yet. It dropped
71% of Monday's
price events at parallelism 1 and
43% of the week's at
parallelism 4. Generating watermarks per partition fixed this. The second bug was rarer: on
Wednesday at 18:56, 5506 events were republished with a time 18 hours
in the past. The specification requires dropping them, but subtasks reading the backlog at
different speeds let the watermark lag hours behind and accepted some of them, which changed
the results of 303 and 502 symbols at
parallelism 4 and 8. Watermark alignment
bounds this lag. Measured before the optimizations below, it cost
18% of throughput at
parallelism 1 and gained
26% at
parallelism 8. Accepting the republished events instead, as a batch computation would,
changes 685457 later
windows of 1812 symbols, a result a stream processor could only reach by
retracting windows it had already emitted.

== Throughput and Scalability <sec-opt>

#figure(
  table(
    columns: (auto, 1fr, 1fr, 1.3fr, auto, auto),
    align: (left, right, right, right, right, right),
    stroke: none,
    table.hline(),
    table.header([$p$], [First\ version], [Final], [Final,\ all Monday], [CPU\ cores], [Skew]),
    table.hline(stroke: 0.5pt),
    [1], [123k], [962k], [931k #sym.plus.minus 16k], [1.7], [1.00],
    [2], [163k], [1417k], [1256k #sym.plus.minus 29k], [2.4], [1.00],
    [4], [300k], [2180k], [1677k #sym.plus.minus 129k], [3.8], [1.07],
    [8], [505k], [1572k], [1681k #sym.plus.minus 277k], [5.0], [1.27],
    table.hline(),
  ),
  caption: [Mean throughput in events/s over 3 runs, on the first
    20M Monday events unless noted. CPU cores and skew (events read
    by the busiest source subtask relative to the mean) are for the final job on all of Monday.],
) <tab-scale>

Flink's task metrics showed where the first version spent its time. The window and EMA sink
task was busy
77% to 86% of the time
while the source waited on it, because the sink committed each result in its own transaction and
wrote only 834 rows/s. Batching the commits raised throughput at
parallelism 1 from 123k to 571k events/s in a single run. The source
then became the bottleneck: it parsed every event although most carry no price, and parsed each
timestamp three times. Skipping non-price events before parsing and parsing timestamps once
brought parallelism 1 to 962k events/s with identical results (@tab-scale).

On all of Monday the final job reads 931k events/s at parallelism 1,
41 times the day's peak rate, and 1677k at parallelism 4
(1.8#sym.times), but no more at parallelism 8. There no task is
saturated: the containers use 5.0 of the 8 cores
and the source is busy 65% of the time but hardly
backpressured. Three effects, which we did not separate, leave the sources idle: watermark
alignment pauses partitions 23% of the time, the long
tail of symbols makes the busiest partition 1.27#sym.times
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
    [25k], [307 ms], [1364 ms], [1479 ms],
    [250k], [222 ms], [330 ms], [341 ms],
    [500k], [231 ms], [307 ms], [320 ms],
    [758k], [222 ms], [316 ms], [317 ms],
    table.hline(),
  ),
  caption: [Time from when a window can be evaluated until all its EMA rows are committed, at
    parallelism 4 and a steady input rate.],
) <tab-latency>

To measure latency, the producer replays at a fixed rate into a running job and logs when it
sends the first price event of each window, the moment the previous window can be evaluated.
Latency is the time from then until the window's last EMA row is committed, so it includes the
5-second out-of-orderness bound and the batched commit. From 250k to
758k events/s the median stays at or below
231 ms and p99 at or below
330 ms (@tab-latency); the
producer could not send faster while sharing the machine with the job. Latency is highest at
the lowest rate, where event time advances slowly and the watermark waits longer for sparse
partitions.

== Resource Utilization

At parallelism 8 the TaskManagers use 4.1
cores, TimescaleDB 0.8 and Kafka almost none,
since it serves the preloaded batches from memory. No component uses the GPU. Each TaskManager
peaks at about
766 MB of its 1 GB, so TaskManager memory grows linearly with parallelism, while Kafka peaks at
0.9 GB, the
JobManager at 1.0 GB
and TimescaleDB at 0.2 GB.
Memory still caused more trouble than CPU: the original windows buffered every event and ran the 1 GB TaskManagers
out of heap, and Flink's default checkpoint storage, capped at 5 MB per task, overflowed after
two days of data at parallelism 1 and left the job in a restart loop. Pre-reducing windows, a
smaller managed memory share and checkpoints on a shared volume fixed both.
None of the scalability runs restarted.

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
