package trading.web;

import io.javalin.Javalin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** HTTP API and live dashboard for EMA results stored in TimescaleDB. */
public final class WebApp {

    private static final String LATEST_QUERY = """
            SELECT symbol, window_end, last_price, ema38, ema100, received_at
            FROM latest_ema_results
            ORDER BY window_end DESC, symbol
            LIMIT ?
            """;
        private static final String SYMBOLS_QUERY = """
            SELECT DISTINCT symbol
            FROM ema_results
            WHERE COALESCE(last_price, 0) <> 0
               OR COALESCE(ema38, 0) <> 0
               OR COALESCE(ema100, 0) <> 0
            ORDER BY symbol
            """;
    private static final String HISTORY_QUERY = """
            SELECT symbol, window_end, last_price, ema38, ema100, received_at
            FROM ema_results
            WHERE symbol = ?
              AND window_end >= ?
              AND window_end <= ?
            ORDER BY window_end
            LIMIT ?
            """;
    private static final String LATEST_WINDOW_QUERY =
            "SELECT max(window_end) FROM ema_results WHERE symbol = ?";

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong durationNanos = new AtomicLong();

    private WebApp() {
        dbUrl = env("DB_URL", "jdbc:postgresql://localhost:5432/trading");
        dbUser = env("DB_USER", "trading");
        dbPassword = env("DB_PASSWORD", "trading");
    }

    public static void main(String[] args) {
        WebApp web = new WebApp();
        int port = Integer.parseInt(env("WEB_PORT", "7070"));
        Javalin app = Javalin.create(config -> {
            config.routes.get("/", ctx -> ctx.html(loadResource("/dashboard.html")));
            config.routes.get("/dashboard.css", ctx -> ctx.contentType("text/css").result(loadResource("/dashboard.css")));
            config.routes.get("/dashboard.js", ctx -> ctx.contentType("application/javascript").result(loadResource("/dashboard.js")));
            config.routes.get("/health", web::health);
            config.routes.get("/api/symbols", web::symbols);
            config.routes.get("/api/trends/latest", web::latest);
            config.routes.get("/api/trends/{symbol}", web::history);
            config.routes.get("/metrics", web::metrics);
        });
        app.start(port);
    }

    private static String loadResource(String resource) {
        try (InputStream input = WebApp.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException(resource + " is missing from the application resources");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not load " + resource, exception);
        }
    }

    private void health(io.javalin.http.Context context) {
        measure(context, () -> {
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
                statement.executeQuery();
                context.json(Map.of("status", "ok"));
            }
        });
    }

    private void latest(io.javalin.http.Context context) {
        measure(context, () -> {
            int limit = limit(context.queryParam("limit"));
            List<Map<String, Object>> rows = new ArrayList<>();
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(LATEST_QUERY)) {
                statement.setInt(1, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        rows.add(row(result));
                    }
                }
            }
            context.json(rows);
        });
    }

    private void symbols(io.javalin.http.Context context) {
        measure(context, () -> {
            List<String> symbols = new ArrayList<>();
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(SYMBOLS_QUERY);
                 ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    symbols.add(result.getString(1));
                }
            }
            context.json(symbols);
        });
    }

    private void history(io.javalin.http.Context context) {
        measure(context, () -> {
            String symbol = context.pathParam("symbol");
            OffsetDateTime to = parseTime(context.queryParam("to"), latestWindowEnd(symbol));
            OffsetDateTime from = parseTime(context.queryParam("from"), to.minus(Duration.ofHours(24)));
            int limit = limit(context.queryParam("limit"));
            List<Map<String, Object>> rows = new ArrayList<>();
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(HISTORY_QUERY)) {
                statement.setString(1, symbol);
                statement.setObject(2, from);
                statement.setObject(3, to);
                statement.setInt(4, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        rows.add(row(result));
                    }
                }
            }
            context.json(rows);
        });
    }

    private OffsetDateTime latestWindowEnd(String symbol) throws Exception {
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(LATEST_WINDOW_QUERY)) {
            statement.setString(1, symbol);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    OffsetDateTime latest = result.getObject(1, OffsetDateTime.class);
                    if (latest != null) {
                        return latest;
                    }
                }
            }
        }
        return OffsetDateTime.now();
    }

    private void metrics(io.javalin.http.Context context) {
        long requestCount = requests.get();
        long failureCount = failures.get();
        double durationSeconds = durationNanos.get() / 1_000_000_000.0;
        context.contentType("text/plain; version=0.0.4").result("""
                # TYPE trading_web_requests_total counter
                trading_web_requests_total %d
                # TYPE trading_web_request_failures_total counter
                trading_web_request_failures_total %d
                # TYPE trading_web_request_duration_seconds_sum counter
                trading_web_request_duration_seconds_sum %.6f
                # TYPE trading_web_request_duration_seconds_count counter
                trading_web_request_duration_seconds_count %d
                """.formatted(requestCount, failureCount, durationSeconds, requestCount));
    }

    private void measure(io.javalin.http.Context context, ThrowingAction action) {
        long start = System.nanoTime();
        requests.incrementAndGet();
        try {
            action.run();
        } catch (Exception exception) {
            failures.incrementAndGet();
            context.status(500).json(Map.of("error", "database request failed"));
        } finally {
            durationNanos.addAndGet(System.nanoTime() - start);
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    private static Map<String, Object> row(ResultSet result) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("symbol", result.getString("symbol"));
        row.put("windowEnd", result.getObject("window_end", OffsetDateTime.class).toString());
        row.put("lastPrice", result.getDouble("last_price"));
        row.put("ema38", result.getDouble("ema38"));
        row.put("ema100", result.getDouble("ema100"));
        row.put("receivedAt", result.getObject("received_at", OffsetDateTime.class).toString());
        return row;
    }

    private static OffsetDateTime parseTime(String value, OffsetDateTime fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("Invalid timestamp: " + value, exception);
        }
    }

    private static int limit(String value) {
        try {
            return Math.max(1, Math.min(1000, value == null ? 100 : Integer.parseInt(value)));
        } catch (NumberFormatException exception) {
            return 100;
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
