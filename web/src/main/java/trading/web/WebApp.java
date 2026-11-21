package trading.web;

import io.javalin.Javalin;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/** HTTP API and trading terminal frontend for EMA results stored in TimescaleDB. */
public final class WebApp {
  // SQL QUERIES
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

    private static final String LATEST_SYMBOL_TIME_QUERY = """
            SELECT MAX(window_end)
            FROM ema_results
            WHERE symbol = ?
            """;

    private static final String LATEST_SIGNALS_QUERY = """
            SELECT symbol, window_end, signal_type, ema38, ema100, received_at
            FROM crossover_events
            ORDER BY window_end DESC
            LIMIT ?
            """;

    private static final String SIGNALS_FOR_SYMBOL_QUERY = """
            SELECT symbol, window_end, signal_type, ema38, ema100, received_at
            FROM crossover_events
            WHERE symbol = ?
              AND window_end >= ?
              AND window_end <= ?
            ORDER BY window_end
            LIMIT ?
            """;

    // Newest rows per requested symbol, used for watchlist values and sparklines.
    private static final String WATCHLIST_QUERY = """
            SELECT symbol, window_end, last_price, ema38, ema100
            FROM (
                SELECT symbol, window_end, last_price, ema38, ema100,
                       row_number() OVER (PARTITION BY symbol ORDER BY window_end DESC) AS position
                FROM ema_results
                WHERE symbol = ANY (?)
            ) ranked
            WHERE position <= ?
            ORDER BY symbol, window_end
            """;

    private static final String WATCHLIST_SIGNALS_QUERY = """
            SELECT DISTINCT ON (symbol) symbol, window_end, signal_type, ema38, ema100, received_at
            FROM crossover_events
            WHERE symbol = ANY (?)
            ORDER BY symbol, window_end DESC
            """;

    private static final String SUMMARY_QUERY = """
            SELECT (SELECT MAX(window_end) FROM ema_results) AS latest_window_end,
                   (SELECT COUNT(*) FROM crossover_events WHERE signal_type = 'BUY') AS buy_signals,
                   (SELECT COUNT(*) FROM crossover_events WHERE signal_type = 'SELL') AS sell_signals
            """;

    private static final int MAX_WATCHLIST_SYMBOLS = 100;
    private static final int MAX_SPARKLINE_POINTS = 288;
    private static final int MAX_HISTORY_ROWS = 5000;
    private static final Pattern ASSET_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
    private static final Map<String, String> ASSET_TYPES = Map.of(
            "js", "application/javascript",
            "css", "text/css",
            "woff2", "font/woff2",
            "woff", "font/woff",
            "svg", "image/svg+xml",
            "png", "image/png",
            "map", "application/json");
    private static final String MISSING_FRONTEND_HTML = """
            <!doctype html>
            <title>Trading Terminal</title>
            <p>The frontend bundle is missing from this build.
            Run <code>npm ci &amp;&amp; npm run build</code> in <code>web/frontend</code>
            (or build without <code>-Dskip.frontend</code>) and restart.</p>
            """;

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

        // Register static files, API endpoints, and metrics.
        Javalin app = Javalin.create(config -> {
            config.routes.get("/", WebApp::index);
            config.routes.get("/assets/{file}", WebApp::asset);
            config.routes.get("/health", web::health);
            config.routes.get("/api/symbols", web::symbols);
            config.routes.get("/api/summary", web::summary);
            config.routes.get("/api/watchlist", web::watchlist);
            config.routes.get("/api/trends/latest", web::latest);
            config.routes.get("/api/trends/{symbol}", web::history);
            config.routes.get("/api/signals/latest", web::latestSignals);
            config.routes.get("/api/signals/{symbol}", web::signalsHistory);
            config.routes.get("/metrics", web::metrics);
        });

        app.start(port);
    }

    // Serves the single-page frontend built by Vite into /static on the classpath.
    private static void index(io.javalin.http.Context context) throws IOException {
        byte[] html = readResource("/static/index.html");
        context.header("Cache-Control", "no-cache");
        if (html == null) {
            context.status(503).html(MISSING_FRONTEND_HTML);
        } else {
            context.contentType("text/html; charset=utf-8").result(html);
        }
    }

    // Serves the content-hashed bundles and fonts that the frontend build emits.
    private static void asset(io.javalin.http.Context context) throws IOException {
        String file = context.pathParam("file");
        byte[] content = ASSET_NAME.matcher(file).matches()
                ? readResource("/static/assets/" + file)
                : null;
        if (content == null) {
            context.status(404).result("Not found");
            return;
        }

        String extension = file.substring(file.lastIndexOf('.') + 1);
        context.contentType(ASSET_TYPES.getOrDefault(extension, "application/octet-stream"));
        context.header("Cache-Control", "public, max-age=31536000, immutable");
        context.result(content);
    }

    private static byte[] readResource(String resource) throws IOException {
        try (InputStream input = WebApp.class.getResourceAsStream(resource)) {
            return input == null ? null : input.readAllBytes();
        }
    }

    // Checks that the TimescaleDB database is reachable
    private void health(io.javalin.http.Context context) {
        measure(context, () -> {
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement("SELECT 1");
                 ResultSet result = statement.executeQuery()) {
                result.next();
                context.json(Map.of("status", "ok"));
            }
        });
    }
    // Returns the newest stored window and the total BUY/SELL signal counts.
    private void summary(io.javalin.http.Context context) {
        measure(context, () -> {
            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(SUMMARY_QUERY);
                 ResultSet result = statement.executeQuery()) {
                result.next();
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("latestWindowEnd", timestamp(result, "latest_window_end"));
                summary.put("buySignals", result.getLong("buy_signals"));
                summary.put("sellSignals", result.getLong("sell_signals"));
                context.json(summary);
            }
        });
    }

    // Returns the newest values, a short price series, and the last signal for
    // each requested symbol in one round trip.
    private void watchlist(io.javalin.http.Context context) {
        measure(context, () -> {
            String[] symbols = symbolList(context.queryParam("symbols"));
            int points = bounded(context.queryParam("points"), 48, 2, MAX_SPARKLINE_POINTS, "points");
            Map<String, Map<String, Object>> entries = new LinkedHashMap<>();
            for (String symbol : symbols) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("symbol", symbol);
                entry.put("rows", new ArrayList<Map<String, Object>>());
                entry.put("lastSignal", null);
                entries.put(symbol, entry);
            }

            if (symbols.length > 0) {
                try (Connection connection = connection()) {
                    try (PreparedStatement statement = connection.prepareStatement(WATCHLIST_QUERY)) {
                        statement.setArray(1, connection.createArrayOf("text", symbols));
                        statement.setInt(2, points);
                        try (ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                Map<String, Object> point = new LinkedHashMap<>();
                                point.put("windowEnd", timestamp(result, "window_end"));
                                point.put("lastPrice", nullableDouble(result, "last_price"));
                                point.put("ema38", nullableDouble(result, "ema38"));
                                point.put("ema100", nullableDouble(result, "ema100"));
                                rowsOf(entries.get(result.getString("symbol"))).add(point);
                            }
                        }
                    }
                    try (PreparedStatement statement =
                                 connection.prepareStatement(WATCHLIST_SIGNALS_QUERY)) {
                        statement.setArray(1, connection.createArrayOf("text", symbols));
                        try (ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                entries.get(result.getString("symbol"))
                                        .put("lastSignal", signalRow(result));
                            }
                        }
                    }
                }
            }

            context.json(new ArrayList<>(entries.values()));
        });
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rowsOf(Map<String, Object> entry) {
        return (List<Map<String, Object>>) entry.get("rows");
    }

    // Returns latest EMA Results
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

    // Returns the list of symbols that have usable EMA data.
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

    // Returns historical EMA data for one symbol and time range.
    private void history(io.javalin.http.Context context) {
        measure(context, () -> {
            String symbol = context.pathParam("symbol");
            OffsetDateTime requestedTo = parseTime(context.queryParam("to"), null);
            OffsetDateTime requestedFrom = parseTime(context.queryParam("from"), null);
            Duration range = Duration.ofHours(
                    bounded(context.queryParam("hours"), 24, 1, 24 * 30, "hours"));
            int limit = bounded(context.queryParam("limit"), 100, 1, MAX_HISTORY_ROWS, "limit");

            List<Map<String, Object>> rows = new ArrayList<>();

            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(HISTORY_QUERY)) {
            OffsetDateTime to = requestedTo == null
                ? latestWindowEnd(connection, symbol)
                : requestedTo;
            if (to == null) {
                to = OffsetDateTime.now();
            }
            OffsetDateTime from = requestedFrom == null
                ? to.minus(range)
                : requestedFrom;

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

    // Returns latest BUY/SELL crossover events.
    private void latestSignals(io.javalin.http.Context context) {
        measure(context, () -> {
            int limit = limit(context.queryParam("limit"));
            List<Map<String, Object>> rows = new ArrayList<>();

            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(LATEST_SIGNALS_QUERY)) {
                statement.setInt(1, limit);

                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        rows.add(signalRow(result));
                    }
                }
            }

            context.json(rows);
        });
    }

    // Returns crossover signals for one symbol and time range.
    private void signalsHistory(io.javalin.http.Context context) {
        measure(context, () -> {
            String symbol = context.pathParam("symbol");
            OffsetDateTime requestedTo = parseTime(context.queryParam("to"), null);
            OffsetDateTime requestedFrom = parseTime(context.queryParam("from"), null);
            Duration range = Duration.ofHours(
                    bounded(context.queryParam("hours"), 24, 1, 24 * 30, "hours"));
            int limit = bounded(context.queryParam("limit"), 100, 1, MAX_HISTORY_ROWS, "limit");

            List<Map<String, Object>> rows = new ArrayList<>();

            try (Connection connection = connection();
                 PreparedStatement statement = connection.prepareStatement(SIGNALS_FOR_SYMBOL_QUERY)) {
                OffsetDateTime to = requestedTo == null
                        ? latestWindowEnd(connection, symbol)
                        : requestedTo;
                if (to == null) {
                    to = OffsetDateTime.now();
                }
                OffsetDateTime from = requestedFrom == null
                        ? to.minus(range)
                        : requestedFrom;

                statement.setString(1, symbol);
                statement.setObject(2, from);
                statement.setObject(3, to);
                statement.setInt(4, limit);

                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        rows.add(signalRow(result));
                    }
                }
            }

            context.json(rows);
        });
    }

    // Finds the newest stored window for a symbol.

    private OffsetDateTime latestWindowEnd(Connection connection, String symbol) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(LATEST_SYMBOL_TIME_QUERY)) {
            statement.setString(1, symbol);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return result.getObject(1, OffsetDateTime.class);
                }
                return null;
            }
        }
    }
    // Exposes request counts, failures, and total request duration.
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
                """.formatted(
                requestCount,
                failureCount,
                durationSeconds,
                requestCount));
    }

    // Runs an endpoint, converts errors to HTTP responses, and records metrics.

    private void measure(
            io.javalin.http.Context context,
            ThrowingAction action) {

        long start = System.nanoTime();
        requests.incrementAndGet();

        try {
            action.run();
        } catch (IllegalArgumentException exception) {
            failures.incrementAndGet();
            context.status(400).json(Map.of("error", exception.getMessage()));
        } catch (Exception exception) {
            failures.incrementAndGet();
            context.status(500).json(Map.of("error", "server request failed"));
        } finally {
            durationNanos.addAndGet(System.nanoTime() - start);
        }
    }


    // Opens a new database connection.
    private Connection connection() throws Exception {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    // Converts a database result row into JSON-friendly values.
    private static Map<String, Object> row(ResultSet result) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("symbol", result.getString("symbol"));
        row.put("windowEnd", timestamp(result, "window_end"));
        row.put("lastPrice", nullableDouble(result, "last_price"));
        row.put("ema38", nullableDouble(result, "ema38"));
        row.put("ema100", nullableDouble(result, "ema100"));
        row.put("receivedAt", timestamp(result, "received_at"));
        return row;
    }

    private static Map<String, Object> signalRow(ResultSet result) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("symbol", result.getString("symbol"));
        row.put("windowEnd", timestamp(result, "window_end"));
        row.put("type", result.getString("signal_type"));
        row.put("ema38", nullableDouble(result, "ema38"));
        row.put("ema100", nullableDouble(result, "ema100"));
        row.put("receivedAt", timestamp(result, "received_at"));
        return row;
    }

    private static String timestamp(ResultSet result, String column) throws Exception {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toString();
    }

    private static Double nullableDouble(ResultSet result, String column) throws Exception {
        double value = result.getDouble(column);
        return result.wasNull() ? null : value;
    }

    // Parses optional timestamps and applies a fallback when absent.
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

    // Parses and constrains the requested result limit.
    private static int limit(String value) {
        return bounded(value, 100, 1, 1000, "limit");
    }

    // Parses an optional integer parameter and clamps it to the allowed range.
    private static int bounded(String value, int fallback, int min, int max, String name) {
        if (value == null || value.isBlank()) {
            return fallback;
        }

        try {
            return Math.max(min, Math.min(max, Integer.parseInt(value)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid " + name + ": " + value, exception);
        }
    }

    // Splits a comma-separated symbol list, dropping blanks and duplicates.
    private static String[] symbolList(String value) {
        if (value == null || value.isBlank()) {
            return new String[0];
        }

        Set<String> symbols = new LinkedHashSet<>();
        for (String symbol : value.split(",")) {
            if (!symbol.isBlank()) {
                symbols.add(symbol.trim());
            }
        }
        if (symbols.size() > MAX_WATCHLIST_SYMBOLS) {
            throw new IllegalArgumentException(
                    "At most " + MAX_WATCHLIST_SYMBOLS + " symbols can be requested");
        }
        return symbols.toArray(String[]::new);
    }

    // Gets an environment variable, or returns a default value if it's not set.
    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }
}
