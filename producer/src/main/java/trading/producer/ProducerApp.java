package trading.producer;

import trading.common.TickEvent;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/** Reads Trading Data CSV files and sends every event to Kafka, keyed by symbol. */
public class ProducerApp {

    private static final long WINDOW_MILLIS = Duration.ofMinutes(5).toMillis();
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final ZoneId ZONE = ZoneId.of("Europe/Paris");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: producer <csv file or directory>...");
            System.exit(1);
        }
        String servers = env("BOOTSTRAP_SERVERS", "localhost:9094");
        String topic = env("TOPIC", TickEvent.TOPIC);
        int partitions = Integer.parseInt(env("PARTITIONS", "4"));
        long rate = Long.parseLong(env("RATE", "0"));
        // For latency measurements: when each window could first be evaluated, i.e. when the first
        // price event at or after its end was sent, as "window end epoch ms,wall clock epoch ms"
        String windowLog = env("WINDOW_LOG", "");
        List<String> windowStarts = new ArrayList<>();
        long lastWindow = Long.MIN_VALUE;

        createTopic(servers, topic, partitions);

        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers,
                ProducerConfig.LINGER_MS_CONFIG, 20,
                ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        AtomicLong failures = new AtomicLong();
        long sent = 0;
        long start = System.nanoTime();

        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer())) {
            for (File file : csvFiles(args)) {
                System.out.println("Sending " + file);
                try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("#") || line.startsWith("ID,")) {
                            continue;
                        }
                        TickEvent event = parse(line);
                        producer.send(new ProducerRecord<>(topic, event.id(), event.toJson()), (metadata, error) -> {
                            if (error != null && failures.getAndIncrement() == 0) {
                                error.printStackTrace();
                            }
                        });
                        sent++;
                        if (!windowLog.isEmpty() && event.last() != null && event.last() > 0) {
                            long window = Math.floorDiv(eventMillis(event), WINDOW_MILLIS);
                            if (window > lastWindow && window != Long.MIN_VALUE) {
                                windowStarts.add(window * WINDOW_MILLIS + "," + System.currentTimeMillis());
                                lastWindow = window;
                            }
                        }

                        if (rate > 0) {
                            long aheadMillis = sent * 1000 / rate - Duration.ofNanos(System.nanoTime() - start).toMillis();
                            if (aheadMillis > 0) {
                                Thread.sleep(aheadMillis);
                            }
                        }
                        if (sent % 1_000_000 == 0) {
                            System.out.println("Sent " + sent + " events");
                        }
                    }
                }
            }
        }

        if (!windowLog.isEmpty()) {
            Files.writeString(Path.of(windowLog), windowStarts.stream().collect(Collectors.joining("\n", "", "\n")));
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("Done: sent %d events in %.1f s (%.0f events/s), %d failures%n",
                sent, seconds, sent / seconds, failures.get());
        if (failures.get() > 0) {
            System.exit(2);
        }
    }

    /** Picks the needed columns from a CSV line, see Table 1 of the project spec for the column order. */
    static TickEvent parse(String line) {
        String[] f = line.split(",", -1);
        return new TickEvent(f[0], f[1], f[2],
                f[3].isBlank() ? null : f[3],
                f[21].isBlank() ? null : Double.valueOf(f[21]),
                f[23].isBlank() ? null : f[23],
                f[26].isBlank() ? null : f[26]);
    }

    /**
     * Event time as the Flink job assigns it: trading date and time, else system date and time.
     * Long.MIN_VALUE for events the job drops because their time is missing or invalid.
     */
    static long eventMillis(TickEvent event) {
        String date = event.tradingDate() != null ? event.tradingDate() : event.date();
        String time = event.tradingTime() != null ? event.tradingTime() : event.time();
        try {
            return LocalDate.parse(date, DATE_FMT).atTime(LocalTime.parse(time)).atZone(ZONE).toInstant().toEpochMilli();
        } catch (RuntimeException exception) {
            return Long.MIN_VALUE;
        }
    }

    /** Directories are expanded to their CSV files, sorted by name so the days are sent in order. */
    private static List<File> csvFiles(String[] args) {
        List<File> files = new ArrayList<>();
        for (String arg : args) {
            File file = new File(arg);
            File[] children = file.listFiles((dir, name) -> name.endsWith(".csv"));
            if (children == null) {
                files.add(file);
            } else {
                Arrays.sort(children);
                files.addAll(Arrays.asList(children));
            }
        }
        return files;
    }

    private static void createTopic(String servers, String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
