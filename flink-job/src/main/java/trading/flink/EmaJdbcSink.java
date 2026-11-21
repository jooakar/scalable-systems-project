package trading.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import trading.common.EmaResult;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

/** Writes windowed EMA results to the TimescaleDB hypertable. */
public final class EmaJdbcSink implements Sink<EmaResult> {

    @Override
    public SinkWriter<EmaResult> createWriter(WriterInitContext context) throws IOException {
        return new EmaJdbcWriter();
    }

    private static final class EmaJdbcWriter implements SinkWriter<EmaResult> {

        private static final String INSERT = """
                INSERT INTO ema_results (window_end, symbol, last_price, ema38, ema100)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (symbol, window_end) DO UPDATE SET
                    last_price = EXCLUDED.last_price,
                    ema38 = EXCLUDED.ema38,
                    ema100 = EXCLUDED.ema100
                """;

        private final Connection connection;
        private final PreparedStatement statement;

        private EmaJdbcWriter() throws IOException {
            try {
                connection = DriverManager.getConnection(
                        env("DB_URL", "jdbc:postgresql://timescaledb:5432/trading"),
                        env("DB_USER", "trading"),
                        env("DB_PASSWORD", "trading"));
                connection.setAutoCommit(true);
                statement = connection.prepareStatement(INSERT);
            } catch (SQLException exception) {
                throw new IOException("Could not connect to TimescaleDB", exception);
            }
        }

        @Override
        public void write(EmaResult result, Context context) throws IOException {
            try {
                statement.setObject(1, OffsetDateTime.parse(result.windowEnd()));
                statement.setString(2, result.symbol());
                statement.setDouble(3, result.lastPrice());
                statement.setDouble(4, result.ema38());
                statement.setDouble(5, result.ema100());
                statement.executeUpdate();
            } catch (SQLException exception) {
                throw new IOException("Could not write EMA result", exception);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Auto-commit makes every result durable immediately for live queries.
        }

        @Override
        public void close() throws Exception {
            statement.close();
            connection.close();
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
