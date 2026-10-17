///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.springframework.batch:spring-batch-core
//SOURCES ../src/corebank/Config.java ../src/corebank/batch/BatchJobRunner.java
//SOURCES corebank/TriggerMonitorTest.java
//SOURCES ../src/corebank/batch/TriggerMonitor.java
//SOURCES ../src/corebank/BatchLocks.java ../src/corebank/output/BalanceExporter.java
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.springframework.boot:spring-boot-starter
//DEPS org.springframework.batch:spring-batch-infrastructure
//DEPS org.postgresql:postgresql
//DEPS org.liquibase:liquibase-core
//DEPS org.springframework:spring-jdbc
//DEPS org.junit.platform:junit-platform-console-standalone:1.11.4
//SOURCES ../src/corebank/BankingRecords.java ../src/corebank/ingest/BatchIngestor.java
//SOURCES ../src/corebank/readers/*.java
//SOURCES ../src/corebank/processing/BatchProcessor.java

//FILES db/changelog/db.changelog-master.xml=../../postgres/changelog/db.changelog-master.xml
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

import org.junit.platform.console.ConsoleLauncher;

public class TriggerMonitorTests {
    public static void main(String[] args) {
        ConsoleLauncher.main("execute", "--select-class=corebank.TriggerMonitorTest", "--fail-if-no-tests");
    }
}
