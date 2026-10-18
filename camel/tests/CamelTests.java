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
//SOURCES ../src/banking/*.java ../src/CamelBanking.java banking/*.java

//DEPS org.junit.platform:junit-platform-console-standalone:1.11.4
//DEPS org.springframework.batch:spring-batch-infrastructure:5.2.2
//DEPS org.springframework:spring-jdbc:6.2.5
//DEPS org.springframework:spring-context:6.2.5
//SOURCES ../../corebank/src/corebank/BankingRecords.java ../../corebank/src/corebank/BatchLocks.java
//SOURCES ../../corebank/src/corebank/readers/*.java
//SOURCES ../../corebank/src/corebank/ingest/BatchIngestor.java
//SOURCES ../../corebank/src/corebank/processing/BatchProcessor.java
//SOURCES ../../corebank/src/corebank/output/BalanceExporter.java

import banking.Settings;
import org.junit.platform.console.ConsoleLauncher;

public class CamelTests {
    public static void main(String[] args) throws Exception {
        String url=System.getenv("COREBANK_TEST_DB_URL");
        if (url == null || !url.endsWith("/corebank_test"))
            throw new IllegalArgumentException("Set COREBANK_TEST_DB_URL to disposable corebank_test; tests erase its data");
        CamelBanking.migrate(new Settings("", "", url, "postgres", System.getenv("COREBANK_TEST_DB_PASSWORD"),null,null,null));
        ConsoleLauncher.main("execute", "--select-class=banking.BankingTest", "--fail-if-no-tests", "--details=tree");
    }
}
