package corebank;

import java.sql.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class OperationalSchemaTest {
    private Connection db;
    @BeforeEach void setup() throws Exception {
        db = DriverManager.getConnection(System.getenv("COREBANK_TEST_DB_URL"),
            System.getenv().getOrDefault("COREBANK_TEST_DB_USER", "postgres"),
            System.getenv("COREBANK_TEST_DB_PASSWORD"));
        db.setAutoCommit(false);
        sql("INSERT INTO core.customer VALUES (901, 'Jane', 'Doe', '1 Main St', 'Toronto', 'ON', 'A1A 1A1', 'Canada', DEFAULT, DEFAULT)");
        sql("INSERT INTO core.account(account_id,start_date,account_type) VALUES (901,'2026-01-01','CHECKING')");
        sql("INSERT INTO core.batch_run(batch_date) VALUES ('2026-10-09')");
    }
    @AfterEach void close() throws Exception { if (db != null) { db.rollback(); db.close(); } }
    private void sql(String sql) throws SQLException { try (var s=db.createStatement()) { s.execute(sql); } }
    private String value(String sql) throws SQLException {
        try (var s=db.createStatement(); var rs=s.executeQuery(sql)) { assertTrue(rs.next()); return rs.getString(1); }
    }
    private void rejected(String sql, String state) throws Exception {
        var save=db.setSavepoint();
        var ex=assertThrows(SQLException.class, () -> sql(sql));
        assertEquals(state, ex.getSQLState()); db.rollback(save);
    }
    private String transaction(long id, long account, String date) {
        return "INSERT INTO core.transaction(transaction_id,account_id,type,amount,effective_date,batch_date,batch_run_id) "
            + "SELECT " + id + "," + account + ",'CR',12345,'2026-10-09','" + date + "',batch_run_id FROM core.batch_run WHERE batch_date='2026-10-09'";
    }
    @Test void initializesBalanceAndRollsBackWithAccount() throws Exception {
        assertEquals("0", value("SELECT balance FROM core.balance WHERE account_id=901"));
        var save=db.setSavepoint();
        sql("INSERT INTO core.account(account_id,start_date,account_type) VALUES (902,'2026-01-01','SAVINGS')");
        assertEquals("0", value("SELECT balance FROM core.balance WHERE account_id=902"));
        db.rollback(save);
        assertEquals("0", value("SELECT count(*) FROM core.balance WHERE account_id=902"));
        sql("UPDATE core.balance SET balance=-100 WHERE account_id=901");
        assertEquals("-100", value("SELECT balance FROM core.balance WHERE account_id=901"));
    }
    @Test void maintainsAuditFields() throws Exception {
        var created=value("SELECT created_at FROM core.customer WHERE customer_id=901");
        var updated=value("SELECT updated_at FROM core.customer WHERE customer_id=901");
        sql("UPDATE core.customer SET first_name='Jo', created_at='2000-01-01', updated_at='2000-01-01' WHERE customer_id=901");
        assertEquals(created,value("SELECT created_at FROM core.customer WHERE customer_id=901"));
        assertNotEquals(updated,value("SELECT updated_at FROM core.customer WHERE customer_id=901"));
    }
    @Test void enforcesRelationshipIdentityAndReferences() throws Exception {
        sql("INSERT INTO core.relationship(account_id,customer_id,type) VALUES(901,901,'PRIMARY')");
        rejected("INSERT INTO core.relationship(account_id,customer_id,type) VALUES(901,901,'SECONDARY')", "23505");
        rejected("INSERT INTO core.relationship(account_id,customer_id,type) VALUES(901,999,'PRIMARY')", "23503");
        rejected("DELETE FROM core.customer WHERE customer_id=901", "23503");
    }
    @Test void rejectsAnyEndDateButPreservesHistoricalTransactions() throws Exception {
        sql(transaction(901,901,"2026-10-09"));
        sql("UPDATE core.account SET end_date='2027-01-01' WHERE account_id=901");
        rejected(transaction(902,901,"2026-10-09"), "23514");
        assertEquals("1", value("SELECT count(*) FROM core.transaction WHERE account_id=901"));
        rejected("UPDATE core.account SET end_date='2025-01-01' WHERE account_id=901", "23514");
    }
    @Test void enforcesImmutableUniqueTransactionsAndMatchingBatchDate() throws Exception {
        sql(transaction(901,901,"2026-10-09"));
        rejected(transaction(901,901,"2026-10-09"), "23505");
        rejected(transaction(902,999,"2026-10-09"), "23503");
        rejected(transaction(902,901,"2026-10-10"), "23503");
        rejected("UPDATE core.transaction SET amount=1 WHERE transaction_id=901", "23514");
        rejected("DELETE FROM core.transaction WHERE transaction_id=901", "23514");
        rejected(transaction(902,901,"2026-10-09").replace("12345", "-1"), "23514");
    }
    @Test void enforcesEffectiveDateAtDatabaseBoundary() throws Exception {
        sql("UPDATE core.account SET start_date='2026-10-10' WHERE account_id=901");
        rejected(transaction(901,901,"2026-10-09"), "23514");
        sql(transaction(901,901,"2026-10-09").replace("12345,'2026-10-09'", "12345,'2026-10-10'"));
        assertEquals("2026-10-10",value("SELECT effective_date FROM core.transaction WHERE transaction_id=901"));
        rejected("UPDATE core.transaction SET effective_date='2026-10-11' WHERE transaction_id=901", "23514");
        rejected(transaction(902,901,"2026-10-09").replace("12345,'2026-10-09'", "12345,NULL"), "23502");
    }
    @Test void migrationBackfillsLegacyDatesWithoutChangingLedgerOrAudit() throws Exception {
        sql(transaction(901,901,"2026-10-09"));
        String created=value("SELECT created_at FROM core.transaction WHERE transaction_id=901");
        sql("INSERT INTO core_ingest.transaction VALUES ('2026-10-09','t.dat',1,902,901,'CR',100,'2026-10-09')");
        sql("ALTER TABLE core.transaction DROP COLUMN effective_date");
        sql("ALTER TABLE core_ingest.transaction DROP COLUMN effective_date");
        try(var stream=getClass().getResourceAsStream("/db/changelog/010-transaction-effective-date.sql")) {
            sql(new String(java.util.Objects.requireNonNull(stream).readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));
        }
        assertEquals("2026-10-09",value("SELECT effective_date FROM core.transaction WHERE transaction_id=901"));
        assertEquals("2026-10-09",value("SELECT effective_date FROM core_ingest.transaction WHERE transaction_id=902"));
        assertEquals("12345",value("SELECT amount FROM core.transaction WHERE transaction_id=901"));
        assertEquals(created,value("SELECT created_at FROM core.transaction WHERE transaction_id=901"));
        rejected("UPDATE core.transaction SET effective_date='2026-10-10' WHERE transaction_id=901", "23514");
    }
    @Test void retainsFailedAttemptsAndPreventsDuplicateSuccessfulDates() throws Exception {
        rejected("INSERT INTO core.batch_run(batch_date) VALUES('2026-10-09')", "23505");
        rejected("UPDATE core.batch_run SET status='COMPLETED'", "23514");
        sql("UPDATE core.batch_run SET status='FAILED', completed_at=clock_timestamp(), error_message='test failure'");
        sql("INSERT INTO core.batch_run(batch_date) VALUES('2026-10-09')");
        sql("UPDATE core.batch_run SET status='COMPLETED', completed_at=clock_timestamp() WHERE status='RUNNING'");
        rejected("INSERT INTO core.batch_run(batch_date) VALUES('2026-10-09')", "23505");
        assertEquals("2", value("SELECT count(*) FROM core.batch_run WHERE batch_date='2026-10-09'"));
    }
}
