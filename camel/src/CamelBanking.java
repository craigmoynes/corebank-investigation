///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//SOURCES ../../corebank/src/corebank/generator/Generate.java ../../corebank/src/corebank/generator/GeneratedRecord.java
//DEPS org.apache.camel:camel-bom:4.18.0@pom
//DEPS org.apache.camel:camel-main
//DEPS org.apache.camel:camel-kafka
//DEPS org.apache.camel:camel-timer
//DEPS org.apache.camel:camel-direct
//DEPS org.apache.camel:camel-log
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//DEPS com.zaxxer:HikariCP:6.2.1
//DEPS org.postgresql:postgresql:42.7.5
//DEPS org.liquibase:liquibase-core:4.29.2
//DEPS org.springframework.boot:spring-boot:3.4.4
//DEPS ch.qos.logback:logback-classic:1.5.18
//FILES logback.xml=../logback.xml
//FILES db/changelog/001-schemas.sql=../../postgres/changelog/001-schemas.sql
//FILES db/changelog/002-core-ingest.sql=../../postgres/changelog/002-core-ingest.sql
//FILES db/changelog/003-core-operational.sql=../../postgres/changelog/003-core-operational.sql
//FILES db/changelog/004-ingestion-receipt.sql=../../postgres/changelog/004-ingestion-receipt.sql
//FILES db/changelog/005-balance-output.sql=../../postgres/changelog/005-balance-output.sql
//FILES db/changelog/006-batch-jobs.sql=../../postgres/changelog/006-batch-jobs.sql
//FILES db/changelog/007-camel.sql=../../postgres/changelog/007-camel.sql
//FILES db/changelog/008-active-business-date.sql=../../postgres/changelog/008-active-business-date.sql
//FILES db/changelog/009-receipt-staged-at.sql=../../postgres/changelog/009-receipt-staged-at.sql
//FILES db/changelog/010-transaction-effective-date.sql=../../postgres/changelog/010-transaction-effective-date.sql
//FILES db/changelog/db.changelog-master.xml=../../postgres/changelog/db.changelog-master.xml
//SOURCES banking/*.java

import org.apache.camel.main.Main;

import banking.BankingRoutes;
import banking.Settings;
import banking.Topics;
import banking.Store;
import banking.DatabasePool;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

public class CamelBanking {
    public static void migrate(Settings settings) throws Exception {
        try (var db = settings.connect();
                var migration = new Liquibase("db/changelog/db.changelog-master.xml",
                        new ClassLoaderResourceAccessor(), new JdbcConnection(db))) {
            migration.update(new liquibase.Contexts());
        }
    }

    public static void main(String[] args) throws Exception {
        boolean debug = false;
        boolean provisionTopics = false;
        for (String arg : args) {
            switch (arg) {
                case "--debug" -> debug = true;
                case "--topics" -> provisionTopics = true;
                default -> throw new IllegalArgumentException("Usage: CamelBanking.java [--debug] [--topics]");
            }
        }
        // Configure before the first logger is created; no Spring application context is needed.
        System.setProperty("PID", Long.toString(ProcessHandle.current().pid()));
        if (debug) System.setProperty("banking.log.level", "DEBUG");
        Settings settings = Settings.fromEnvironment();
        if (provisionTopics) {
            Topics.provision(settings.brokers());
            return;
        }
        Topics.verify(settings.brokers());
        migrate(settings);
        // One route application per database, irrespective of the chosen Kafka group.
        try (var lease = settings.connect(); var statement = lease.createStatement()) {
            try (var rows = statement.executeQuery("SELECT pg_try_advisory_lock(64019,0)")) {
                rows.next();
                if (!rows.getBoolean(1))
                    throw new IllegalStateException("A Camel banking application is already running");
            }
            // Keep the session advisory-lock lease outside the pool.
            try (var pool = DatabasePool.create(settings)) {
                Main main = new Main();
                try {
                    main.configure().addRoutesBuilder(new BankingRoutes(settings,new Store(pool)));
                    main.run();
                } finally { main.stop(); }
            }
        }
    }
}
