package banking;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public record Settings(String brokers, String group, String url, String user, String password,
        Path input, Path output, Path error) {
    public static Settings fromEnvironment() {
        return new Settings(env("CAMEL_BROKERS", "localhost:19092"), env("CAMEL_GROUP", "corebank-camel-v1"),
                env("COREBANK_DB_URL", "jdbc:postgresql://localhost:5432/corebank"),
                env("COREBANK_DB_USER", "postgres"),
                env("COREBANK_DB_PASSWORD", System.getenv("POSTGRES_PASSWORD")),
                Path.of(env("COREBANK_INPUT_DIRECTORY", "data/input")),
                Path.of(env("COREBANK_OUTPUT_DIRECTORY", "data/output")),
                Path.of(env("COREBANK_ERROR_DIRECTORY", "data/error")));
    }

    private static String env(String key, String fallback) {
        return System.getenv().getOrDefault(key, fallback);
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
