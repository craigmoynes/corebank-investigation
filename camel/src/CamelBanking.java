///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//SOURCES ../../corebank/src/corebank/generator/Generate.java ../../corebank/src/corebank/generator/GeneratedRecord.java
//DEPS org.apache.camel:camel-bom:4.18.0@pom
//DEPS org.apache.camel:camel-main
//DEPS org.apache.camel:camel-kafka
//DEPS org.apache.camel:camel-timer
//DEPS org.apache.camel:camel-direct
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//DEPS org.postgresql:postgresql:42.7.5
//DEPS org.liquibase:liquibase-core:4.29.2
//DEPS org.slf4j:slf4j-simple:2.0.17
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
        Settings settings = Settings.fromEnvironment();
        if (args.length == 1 && args[0].equals("--topics")) {
            Topics.provision(settings.brokers());
            return;
        }
        if (args.length != 0)
            throw new IllegalArgumentException("Usage: CamelBanking.java [--topics]");
        Topics.verify(settings.brokers());
        migrate(settings);
        // One route application per database, irrespective of the chosen Kafka group.
        try (var lease = settings.connect(); var statement = lease.createStatement()) {
            try (var rows = statement.executeQuery("SELECT pg_try_advisory_lock(64019,0)")) {
                rows.next();
                if (!rows.getBoolean(1))
                    throw new IllegalStateException("A Camel banking application is already running");
            }
            Main main = new Main();
            main.configure().addRoutesBuilder(new BankingRoutes(settings));
            main.run();
        }
    }
}
