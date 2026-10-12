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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/** Reads Trading Data CSV files and sends every event to Kafka, keyed by symbol. */
public class ProducerApp {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: producer <csv file or directory>...");
            System.exit(1);
        }
        String servers = env("BOOTSTRAP_SERVERS", "localhost:9094");
        String topic = env("TOPIC", TickEvent.TOPIC);
        int partitions = Integer.parseInt(env("PARTITIONS", "4"));
        long rate = Long.parseLong(env("RATE", "0"));

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
        return new TickEvent(f[0], f[1], f[2], f[3],
                f[21].isEmpty() ? null : Double.valueOf(f[21]),
                f[23].isEmpty() ? null : f[23],
                f[26].isEmpty() ? null : f[26]);
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
