package trading.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
 * → EMA-38 and EMA-100 per symbol → print to stdout.
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

        // ── Kafka Source ──────────────────────────────────────────────────────
        KafkaSource<TickEvent> source = KafkaSource.<TickEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TickEvent.TOPIC)
                .setGroupId("flink-trend-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TickEventDeserializer())
                .build();

        // Event-time processing: timestamps are extracted from the data itself.
        // We allow up to 5 seconds of out-of-orderness to handle late arrivals,
        // and mark partitions idle if no events arrive for 1 minute so the watermark keeps progressing.
        WatermarkStrategy<TickEvent> watermarks = WatermarkStrategy
                .<TickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> {
                  try {
                    return extractEpochMillis(event);
                  } catch (DateTimeParseException exception) {
                    return recordTimestamp >= 0 ? recordTimestamp : 0L;
                    }
                  })
                .withIdleness(Duration.ofMinutes(1));

        DataStream<TickEvent> ticks = env.fromSource(
                source, watermarks, "kafka-ticks");

        // ── Query 1: EMA computation ──────────────────────────────────────────
        DataStream<EmaResult> emas = ticks
                .filter(e -> e.last() != null)           // only price events
                .keyBy(TickEvent::id)                    // group by symbol
                .window(TumblingEventTimeWindows.of(
                        Duration.ofMinutes(5)))          // 5-minute tumbling windows
                .process(new EmaWindowFunction());

        emas.print("EMA");

        env.execute("Trading Trend Detection");
    }

    /**
     * Extracts epoch milliseconds from a TickEvent's date and time fields.
     *
     * <p>Date format: {@code DD-MM-YYYY} (e.g. {@code 08-11-2021})<br>
     * Time format: {@code HH:mm:ss[.ssss]} parsed via standard ISO local time.</p>
     */
    static long extractEpochMillis(TickEvent event) {
        String dateStr = event.tradingDate() != null
                ? event.tradingDate() : event.date();
        String timeStr = event.tradingTime() != null
                ? event.tradingTime() : event.time();
        LocalDate date = LocalDate.parse(dateStr, DATE_FMT);
        LocalTime time = timeStr != null ? LocalTime.parse(timeStr) : LocalTime.MIN;
        ZonedDateTime zdt = ZonedDateTime.of(date, time, ZONE);
        return zdt.toInstant().toEpochMilli();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
