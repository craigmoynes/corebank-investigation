package banking;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/** Application-owned pool for route transactions; migration and lease stay dedicated. */
public final class DatabasePool {
    private DatabasePool() {}
    public static HikariDataSource create(Settings settings) {
        var config = new HikariConfig();
        config.setPoolName("camel-banking");
        config.setJdbcUrl(settings.url());
        config.setUsername(settings.user());
        config.setPassword(settings.password());
        // Five Kafka routes and two timer routes; no additional processing concurrency.
        config.setMaximumPoolSize(7);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000);
        return new HikariDataSource(config);
    }
}
