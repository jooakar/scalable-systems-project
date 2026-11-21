package trading.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import trading.common.CrossoverEvent;
import trading.common.EmaResult;
import trading.common.TickEvent;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Flink streaming job: Kafka → filter price events → 5-min tumbling windows
 * → EMA-38 and EMA-100 per symbol (Query 1) → TimescaleDB & crossover detection (Query 2).
 *
 * <p>Environment variables:
 * <ul>
 *   <li>{@code BOOTSTRAP_SERVERS} — Kafka brokers (default: {@code kafka:9092})</li>
 * </ul>
 */
public final class TrendJob {

    /** CEST timezone used throughout the Trading Data set. */
    private static final ZoneId ZONE = ZoneId.of("Europe/Paris");
    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("BOOTSTRAP_SERVERS", "kafka:9092");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint state periodically so EMA and crossover state survive failures
        env.enableCheckpointing(60_000);

        // ── Kafka Source ──────────────────────────────────────────────────────
        KafkaSource<TickEvent> source = KafkaSource.<TickEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TickEvent.TOPIC)
                .setGroupId("flink-trend-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TickEventDeserializer())
                .build();

        // Event-time processing: timestamps are extracted from price events only.
        // We allow up to 5 seconds of out-of-orderness to handle late arrivals,
        // and mark partitions idle if no events arrive for 1 minute.
        WatermarkStrategy<TickEvent> watermarks = WatermarkStrategy
                .<TickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> extractEpochMillis(event))
                .withIdleness(Duration.ofMinutes(1));

        // ── Ingest and Filter ─────────────────────────────────────────────────
        // Drop any non-price events and any events without a valid timestamp immediately,
        // so watermarks and windows are only computed over legitimate price observations.
        DataStream<EmaResult> emas = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "kafka-ticks")
                .filter(TrendJob::isUsablePriceEvent)
                .assignTimestampsAndWatermarks(watermarks)
                .keyBy(TickEvent::id)                    // group by symbol
                .window(TumblingEventTimeWindows.of(
                        Duration.ofMinutes(5)))          // 5-minute tumbling windows
                .process(new EmaWindowFunction());

        emas.sinkTo(new EmaJdbcSink()).name("timescaledb-ema-results");

        // ── Query 2: Crossover detection (buy/sell advisories) ────────────────
        DataStream<CrossoverEvent> crossovers = emas
                .keyBy(EmaResult::symbol)
                .process(new CrossoverDetector());

        crossovers.sinkTo(new CrossoverJdbcSink()).name("timescaledb-crossover-events");

        env.execute("Trading Trend Detection");
    }

    /** Returns true only for price events that have valid, parseable timestamps. */
    static boolean isUsablePriceEvent(TickEvent event) {
        if (event == null || event.last() == null) {
            return false;
        }
        String dateStr = event.tradingDate() != null && !event.tradingDate().isBlank()
                ? event.tradingDate() : event.date();
        String timeStr = event.tradingTime() != null && !event.tradingTime().isBlank()
                ? event.tradingTime() : event.time();
        if (dateStr == null || dateStr.isBlank() || timeStr == null || timeStr.isBlank()) {
            return false;
        }
        try {
            LocalDate.parse(dateStr, DATE_FMT);
            LocalTime.parse(timeStr);
            return true;
        } catch (DateTimeParseException exception) {
            return false;
        }
    }

    /**
     * Extracts epoch milliseconds from a TickEvent's date and time fields.
     *
     * <p>Date format: {@code DD-MM-YYYY} (e.g. {@code 08-11-2021})<br>
     * Time format: {@code HH:mm:ss[.ssss]} parsed via standard ISO local time.</p>
     */
    static long extractEpochMillis(TickEvent event) {
        String dateStr = event.tradingDate() != null && !event.tradingDate().isBlank()
                ? event.tradingDate() : event.date();
        String timeStr = event.tradingTime() != null && !event.tradingTime().isBlank()
                ? event.tradingTime() : event.time();
        LocalDate date = LocalDate.parse(dateStr, DATE_FMT);
        LocalTime time = LocalTime.parse(timeStr);
        ZonedDateTime zdt = ZonedDateTime.of(date, time, ZONE);
        return zdt.toInstant().toEpochMilli();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
