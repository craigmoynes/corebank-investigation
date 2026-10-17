package corebank;

import corebank.ingest.BatchIngestor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class BatchIngestorTest {
    @TempDir Path input;
    private Connection db;
    private final LocalDate date = LocalDate.of(2026, 10, 9);
    private final BatchIngestor ingestor = new BatchIngestor();

    @BeforeEach void connect() throws Exception {
        db = DriverManager.getConnection(System.getenv("COREBANK_TEST_DB_URL"),
            System.getenv().getOrDefault("COREBANK_TEST_DB_USER", "postgres"),
            System.getenv("COREBANK_TEST_DB_PASSWORD"));
        try (var s = db.createStatement()) {
            for (String table : new String[] {"customer", "account", "relationship", "transaction"})
                s.executeUpdate("DELETE FROM core_ingest." + table);
        }
    }
    @AfterEach void disconnect() throws Exception { if (db != null) db.close(); }

    private void files() throws Exception {
        Files.writeString(input.resolve("customer_20261009.dat"), String.format(
            "%19d%-10s%-10s%-20s%-10s%-2s%-7s%-10s\n", 1, "José", "Doe", "123 Main St", "Toronto", "ON", "A1B 2C3", "Canada"));
        Files.writeString(input.resolve("account_20261009.dat"), String.format(
            "%19d%-10s%-10s%-8s\n", 2, "2026-10-09", "", "CHECKING"));
        Files.writeString(input.resolve("relationship_20261009.dat"), String.format("%19d%19d%-9s\n", 2, 1, "PRIMARY"));
        Files.writeString(input.resolve("transaction_20261009.dat"), transaction(3, 12345));
    }
    private String transaction(long id, long amount) {
        return String.format("%19d%19d%-6s%20d2026-10-09\n", id, 2, "CR", amount);
    }
    private long count(String table) throws Exception {
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT count(*) FROM core_ingest." + table)) {
            r.next(); return r.getLong(1);
        }
    }
    @Test void loadsAllTypesAndMetadata() throws Exception {
        files();
        var counts = ingestor.ingest(db, input, date);
        for (String table : counts.keySet()) { assertEquals(1L, counts.get(table)); assertEquals(1, count(table)); }
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT * FROM core_ingest.customer")) {
            assertTrue(r.next()); assertEquals("José", r.getString("first_name"));
            assertEquals(date, r.getObject("batch_date", LocalDate.class));
            assertEquals("customer_20261009.dat", r.getString("source_file")); assertEquals(1, r.getLong("source_line"));
        }
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT * FROM core_ingest.account")) {
            r.next(); assertNull(r.getObject("end_date")); assertEquals("CHECKING", r.getString("account_type"));
        }
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT amount FROM core_ingest.transaction")) {
            r.next(); assertEquals(12345, r.getLong(1));
        }
    }
    @Test void rerunReplacesDateAndPreservesOtherDates() throws Exception {
        files(); ingestor.ingest(db, input, date);
        try (var s = db.createStatement()) {
            s.executeUpdate("INSERT INTO core_ingest.transaction SELECT batch_date - 1, source_file, source_line, transaction_id, account_id, type, amount, effective_date FROM core_ingest.transaction");
        }
        Files.writeString(input.resolve("transaction_20261009.dat"), transaction(4, 99));
        ingestor.ingest(db, input, date); ingestor.ingest(db, input, date);
        assertEquals(1, count("customer")); assertEquals(2, count("transaction"));
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT amount FROM core_ingest.transaction WHERE batch_date = DATE '2026-10-09'")) {
            r.next(); assertEquals(99, r.getLong(1));
        }
    }
    @Test void failureAfterChunkRollsBackAllFourFilesAndPreservesPreviousLoad() throws Exception {
        files(); ingestor.ingest(db, input, date);
        Files.writeString(input.resolve("customer_20261009.dat"), "");
        Files.writeString(input.resolve("transaction_20261009.dat"), transaction(5, 20).repeat(501) + "bad\n");
        assertThrows(Exception.class, () -> ingestor.ingest(db, input, date));
        assertTrue(db.getAutoCommit());
        for (String table : new String[] {"customer", "account", "relationship", "transaction"}) assertEquals(1, count(table));
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT transaction_id FROM core_ingest.transaction")) {
            r.next(); assertEquals(3, r.getLong(1));
        }
    }
    @Test void missingFileDoesNotChangeStaging() throws Exception {
        files(); ingestor.ingest(db, input, date);
        Files.delete(input.resolve("relationship_20261009.dat"));
        assertThrows(IllegalArgumentException.class, () -> ingestor.ingest(db, input, date));
        assertEquals(1, count("customer")); assertTrue(db.getAutoCommit());
    }
    @Test void emptyFilesReplacePreviousRows() throws Exception {
        files(); ingestor.ingest(db, input, date);
        for (String table : new String[] {"customer", "account", "relationship", "transaction"})
            Files.writeString(input.resolve(table + "_20261009.dat"), "");
        var counts = ingestor.ingest(db, input, date);
        for (String table : counts.keySet()) { assertEquals(0L, counts.get(table)); assertEquals(0, count(table)); }
    }
    @Test void duplicateBusinessKeysRemainAvailableForValidationAcrossChunks() throws Exception {
        files();
        Files.writeString(input.resolve("transaction_20261009.dat"), transaction(Long.MAX_VALUE, Long.MAX_VALUE).repeat(501));
        ingestor.ingest(db, input, date); assertEquals(501, count("transaction"));
        try (var s = db.createStatement(); var r = s.executeQuery("SELECT max(source_line), max(amount) FROM core_ingest.transaction")) {
            r.next(); assertEquals(501, r.getLong(1)); assertEquals(Long.MAX_VALUE, r.getLong(2));
        }
    }
}
