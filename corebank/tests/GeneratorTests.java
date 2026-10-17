///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//SOURCES ../../camel/src/banking/Records.java
//SOURCES ../src/corebank/BatchLocks.java
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.springframework.boot:spring-boot-starter
//DEPS org.springframework.batch:spring-batch-infrastructure
//DEPS org.postgresql:postgresql
//DEPS org.liquibase:liquibase-core
//DEPS org.springframework:spring-jdbc
//DEPS org.junit.platform:junit-platform-console-standalone:1.11.4
//SOURCES ../src/corebank/BankingRecords.java ../src/corebank/ingest/BatchIngestor.java
//SOURCES ../src/corebank/readers/*.java
//SOURCES corebank/GeneratorTest.java
//SOURCES ../src/corebank/generator/BatchGenerator.java ../src/corebank/generator/Generate.java ../src/corebank/generator/GeneratedRecord.java ../src/corebank/generator/FileGenerator.java
//FILES generator/firstnames.txt=../src/resources/firstnames.txt
//FILES generator/lastnames.txt=../src/resources/lastnames.txt
//FILES generator/streets.txt=../src/resources/streets.txt

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

import org.junit.platform.console.ConsoleLauncher;
import liquibase.integration.spring.SpringLiquibase;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class GeneratorTests {
    public static void main(String[] args) throws Exception {
        if (System.getenv("COREBANK_TEST_DB_URL") == null)
            throw new IllegalArgumentException("Set COREBANK_TEST_DB_URL to a disposable database; tests delete ALL operational and staging rows.");
        var migration = new SpringLiquibase();
        migration.setDataSource(new DriverManagerDataSource(System.getenv("COREBANK_TEST_DB_URL"),
            System.getenv().getOrDefault("COREBANK_TEST_DB_USER", "postgres"),
            System.getenv("COREBANK_TEST_DB_PASSWORD")));
        migration.setChangeLog("classpath:db/changelog/db.changelog-master.xml");
        migration.afterPropertiesSet();
        ConsoleLauncher.main("execute", "--select-class=corebank.GeneratorTest", "--fail-if-no-tests");
    }
}
