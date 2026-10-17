///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.springframework.batch:spring-batch-core
//SOURCES corebank/batch/BatchJobRunner.java corebank/batch/TriggerMonitor.java
//SOURCES corebank/BatchLocks.java corebank/output/BalanceExporter.java
//DEPS org.postgresql:postgresql
//DEPS org.liquibase:liquibase-core
//DEPS org.springframework:spring-jdbc
//SOURCES corebank/ingest/BatchIngestor.java corebank/processing/BatchProcessor.java
//SOURCES corebank/readers/RelationshipFileReader.java corebank/readers/TransactionFileReader.java
//SOURCES corebank/readers/FileReaderSupport.java corebank/readers/AccountFileReader.java
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.springframework.boot:spring-boot-starter
//DEPS org.projectlombok:lombok
//DEPS org.springframework.batch:spring-batch-infrastructure
//SOURCES corebank/CoreApplication.java corebank/Config.java corebank/BankingRecords.java
//SOURCES corebank/readers/CodePointLineTokenizer.java corebank/readers/CustomerFileReader.java
//FILES application.yaml=../application.yaml
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

import corebank.CoreApplication;

/** JBang entry point; Spring components live in the corebank package. */
public class Core {
    public static void main(String[] args) {
        CoreApplication.main(args);
    }
}
