package trading.flink;

import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Makes the PostgreSQL JDBC driver available to a sink for the lifetime of the job. */
final class PostgresDriver {

    private PostgresDriver() {
    }

    /**
     * DriverManager only finds drivers loaded by Flink's user-code classloader. The driver registers
     * itself globally, which would keep every finished job's classes in metaspace, so it is
     * unregistered again when Flink releases the job's classloader.
     */
    static void load(WriterInitContext context) throws IOException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException exception) {
            throw new IOException("PostgreSQL driver missing from the job jar", exception);
        }
        context.getUserCodeClassLoader()
                .registerReleaseHookIfAbsent("deregister-postgres-driver", PostgresDriver::deregister);
    }

    private static void deregister() {
        ClassLoader loader = PostgresDriver.class.getClassLoader();
        DriverManager.drivers().filter(driver -> driver.getClass().getClassLoader() == loader).forEach(driver -> {
            try {
                DriverManager.deregisterDriver(driver);
            } catch (SQLException ignored) {
                // The classloader is being released either way
            }
        });
    }
}
