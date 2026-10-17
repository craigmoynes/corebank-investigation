package corebank;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.item.ExecutionContext;

import corebank.generator.BatchGenerator;
import corebank.generator.BatchGenerator.Request;
import corebank.ingest.BatchIngestor;
import corebank.readers.*;

class GeneratorTest {
    @TempDir
    Path directory;
    Connection db;
    LocalDate date = LocalDate.of(2026, 10, 9);
    BatchGenerator generator = new BatchGenerator();

    @BeforeEach
    void connect() throws Exception {
        db = DriverManager.getConnection(System.getenv("COREBANK_TEST_DB_URL"), "postgres",
                System.getenv("COREBANK_TEST_DB_PASSWORD"));
        try (var s = db.createStatement()) {
            s.execute(
                    "TRUNCATE core_output.balance,core_output.batch_snapshot,core.transaction,core.relationship,core.balance,core.account,core.customer,core.batch_run,core_ingest.customer,core_ingest.account,core_ingest.relationship,core_ingest.transaction");
        }
    }

    @AfterEach
    void close() throws Exception {
        db.close();
    }

    void sql(String text) throws Exception {
        try (var s = db.createStatement()) {
            s.execute(text);
        }
    }

    Path file(String type) {
        return directory.resolve(type + "_20261009.dat");
    }

    @Test
    void generatesSelfContainedBatchThatReadersAndIngestionAccept() throws Exception {
        generator.generate(db, directory, new Request(date, 10, 10, 10, 10, 42));
        var counts = new BatchIngestor().ingest(db, directory, date);
        assertEquals(4, counts.size());
        for (long count : counts.values())
            assertEquals(10, count);
        try (var s = db.createStatement();
                var rows = s.executeQuery(
                        "SELECT count(*) FROM core_ingest.transaction t LEFT JOIN core_ingest.account a USING(account_id) WHERE a.account_id IS NULL")) {
            rows.next();
            assertEquals(0, rows.getInt(1));
        }
        try (var s = db.createStatement();
                var rows = s.executeQuery(
                        "SELECT count(*) FROM core_ingest.relationship r LEFT JOIN core_ingest.customer c USING(customer_id) LEFT JOIN core_ingest.account a USING(account_id) WHERE c.customer_id IS NULL OR a.account_id IS NULL")) {
            rows.next();
            assertEquals(0, rows.getInt(1));
        }
        try (var s = db.createStatement(); var rows = s.executeQuery("SELECT count(*) FROM core.account")) {
            rows.next();
            assertEquals(0, rows.getInt(1));
        }
    }

    @Test
    void transactionsUseOnlyEligibleExistingAccountsAndOthersAreEmpty() throws Exception {
        sql("INSERT INTO core.account(account_id,start_date,end_date,account_type) VALUES (1,'2026-01-01',NULL,'CHECKING'),(2,'2026-01-01','2027-01-01','CHECKING'),(3,'2027-01-01',NULL,'CHECKING')");
        generator.generate(db, directory, new Request(date, 0, 0, 0, 25, 42));
        for (String type : new String[] { "customer", "account", "relationship" })
            assertEquals(0, Files.size(file(type)));
        var reader = TransactionFileReader.create(file("transaction"));
        reader.open(new ExecutionContext());
        try {
            int n = 0;
            BankingRecords.Transaction row;
            while ((row = reader.read()) != null) {
                assertEquals(1, row.accountId());
                assertEquals(date, row.effectiveDate());
                n++;
            }
            assertEquals(25, n);
        } finally {
            reader.close();
        }
    }

    @Test
    void missingDependenciesAndTooManyRelationshipsPublishNothing() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> generator.generate(db, directory, new Request(date, 0, 0, 0, 1, 42)));
        assertThrows(IllegalArgumentException.class,
                () -> generator.generate(db, directory, new Request(date, 1, 1, 2, 0, 42)));
        try (var files = Files.list(directory)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void avoidsExistingRelationshipsAndHandlesExhaustion() throws Exception {
        sql("INSERT INTO core.customer(customer_id,first_name,last_name,address_line1,city,province,postal_code,country) VALUES(1,'Jane','Doe','1 Main','Toronto','ON','A1A 1A1','Canada')");
        sql("INSERT INTO core.account(account_id,start_date,account_type) VALUES(1,'2026-01-01','CHECKING'),(2,'2026-01-01','SAVINGS')");
        sql("INSERT INTO core.relationship(account_id,customer_id,type) VALUES(1,1,'PRIMARY')");
        generator.generate(db, directory, new Request(date, 0, 0, 1, 0, 42));
        var reader = RelationshipFileReader.create(file("relationship"));
        reader.open(new ExecutionContext());
        try {
            assertEquals(2, reader.read().accountId());
            assertNull(reader.read());
        } finally {
            reader.close();
        }
        assertThrows(IllegalArgumentException.class,
                () -> generator.generate(db, directory.resolve("too-many"), new Request(date, 0, 0, 2, 0, 42)));
    }

    @Test
    void seedIsReproducibleAndExistingFilesArePreserved() throws Exception {
        var request = new Request(date, 2, 2, 2, 2, 99);
        generator.generate(db, directory, request);
        generator.generate(db, directory.resolve("repeat"), request);
        for (String type : new String[] { "customer", "account", "relationship", "transaction" })
            assertArrayEquals(Files.readAllBytes(file(type)),
                    Files.readAllBytes(directory.resolve("repeat").resolve(file(type).getFileName())));
        var original = Files.readAllBytes(file("customer"));
        assertThrows(IllegalArgumentException.class, () -> generator.generate(db, directory, request));
        assertArrayEquals(original, Files.readAllBytes(file("customer")));
    }

    @Test
    void sharedRecordsMatchFilesAndCamelJsonForEveryType() throws Exception {
        var request=new Request(date,20,20,20,20,42);
        generator.generate(db,directory,request);
        var source=new corebank.generator.Generate().prepare(db,
            new corebank.generator.Generate.Request(date,20,20,20,20,42));
        var lines=new java.util.LinkedHashMap<String,java.util.List<String>>();
        source.writeTo(record -> {
            var decoded=banking.Records.decode(record.kind(),banking.Records.JSON.writeValueAsString(record.fields()));
            assertEquals(record.key(),decoded.key());
            assertEquals(record.line(),decoded.line());
            lines.computeIfAbsent(record.kind(),k -> new java.util.ArrayList<>()).add(record.line());
        });
        for(var entry:lines.entrySet()) assertEquals(Files.readAllLines(file(entry.getKey())),entry.getValue());
        assertEquals(4,lines.size());
        assertThrows(IllegalStateException.class,() -> source.writeTo(record -> {}));
    }

    @Test
    void validatesCountsDatesAndUnicodeWidths() {
        assertThrows(IllegalArgumentException.class, () -> new Request(date, -1, 0, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new Request(LocalDate.of(10000, 1, 1), 0, 0, 0, 0, 1));
        assertEquals("José ", BatchGenerator.field("Jose\u0301", 5));
        assertEquals(3, BatchGenerator.field("𐐀", 3).codePointCount(0, BatchGenerator.field("𐐀", 3).length()));
        assertThrows(IllegalArgumentException.class, () -> BatchGenerator.field("too long", 2));
        assertThrows(IllegalArgumentException.class, () -> BatchGenerator.field("a\nb", 10));
    }
}
