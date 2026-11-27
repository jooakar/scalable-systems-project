package trading.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.common.eventtime.Watermark;
import trading.common.EmaResult;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

/**
 * Writes windowed EMA results to the TimescaleDB hypertable in batched transactions.
 * A batch is committed when it is full, when a watermark passes (right after windows fire,
 * so live results stay current) and on checkpoints (so no acknowledged result is lost).
 */
public final class EmaJdbcSink implements Sink<EmaResult> {

    @Override
    public SinkWriter<EmaResult> createWriter(WriterInitContext context) throws IOException {
        PostgresDriver.load(context);
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

        private static final int MAX_BATCH = 1000;

        private final Connection connection;
        private final PreparedStatement statement;
        private int pending;

        private EmaJdbcWriter() throws IOException {
            try {
                connection = DriverManager.getConnection(
                        env("DB_URL", "jdbc:postgresql://timescaledb:5432/trading"),
                        env("DB_USER", "trading"),
                        env("DB_PASSWORD", "trading"));
                connection.setAutoCommit(false);
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
                statement.addBatch();
            } catch (SQLException exception) {
                throw new IOException("Could not write EMA result", exception);
            }
            if (++pending >= MAX_BATCH) {
                commit();
            }
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException {
            commit();
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            commit();
        }

        private void commit() throws IOException {
            if (pending == 0) {
                return;
            }
            try {
                statement.executeBatch();
                connection.commit();
                pending = 0;
            } catch (SQLException exception) {
                throw new IOException("Could not write EMA results", exception);
            }
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
