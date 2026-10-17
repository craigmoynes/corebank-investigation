package corebank;

import corebank.processing.BatchProcessor;
import corebank.ingest.BatchIngestor;
import java.sql.*;
import java.time.LocalDate;
import java.nio.file.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class BatchProcessorTest {
    private Connection db;
    private final LocalDate day = LocalDate.of(2026,10,9);
    @TempDir Path input;
    private Connection connect() throws SQLException {
        return DriverManager.getConnection(System.getenv("COREBANK_TEST_DB_URL"),
            System.getenv().getOrDefault("COREBANK_TEST_DB_USER","postgres"), System.getenv("COREBANK_TEST_DB_PASSWORD"));
    }
    @BeforeEach void setup() throws Exception { db=connect(); clean(); }
    @AfterEach void close() throws Exception { if(db!=null) { clean(); db.close(); } }
    private void clean() throws SQLException {
        sql("TRUNCATE core_output.balance,core_output.batch_snapshot,core.transaction,core.relationship,core.balance,core.account,core.customer,core.batch_run,core_ingest.customer,core_ingest.account,core_ingest.relationship,core_ingest.transaction,core_ingest.batch_receipt");
    }
    private void sql(String text) throws SQLException { try(var s=db.createStatement()) { s.execute(text); } }
    private String value(String text) throws SQLException {
        try(var s=db.createStatement();var rs=s.executeQuery(text)) { assertTrue(rs.next());return rs.getString(1); }
    }
    private void receipt(LocalDate d) throws SQLException { sql("INSERT INTO core_ingest.batch_receipt(batch_date) VALUES ('"+d+"')"); }
    private void account(LocalDate d,long id) throws SQLException {
        sql("INSERT INTO core_ingest.account VALUES ('"+d+"','account.dat',"+id+","+id+",'2026-01-01',NULL,'CHECKING')");
    }
    private void tx(LocalDate d,long line,long id,String type,long amount) throws SQLException {
        sql("INSERT INTO core_ingest.transaction VALUES ('"+d+"','transaction.dat',"+line+","+id+",2,'"+type+"',"+amount+",'2026-10-09')");
    }
    private void fixture() throws SQLException {
        receipt(day);account(day,2);
        sql("INSERT INTO core_ingest.customer VALUES ('2026-10-09','customer.dat',1,1,'Jane','Doe','1 Main','Toronto','ON','A1A 1A1','Canada')");
        sql("INSERT INTO core_ingest.relationship VALUES ('2026-10-09','relationship.dat',1,2,1,'PRIMARY')");
    }
    private java.util.Map<String,Long> process(LocalDate d) throws SQLException { return new BatchProcessor().process(db,d); }
    @Test void postsAllTypesCreditsDebitsAndZeroBalances() throws Exception {
        fixture();account(day,3);tx(day,1,10,"CR",100);tx(day,2,11,"DR",150);
        var counts=process(day);
        assertEquals(2L,counts.get("account"));assertEquals(2L,counts.get("transaction"));
        assertEquals("-50",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("0",value("SELECT balance FROM core.balance WHERE account_id=3"));
        assertEquals("COMPLETED",value("SELECT status FROM core.batch_run"));
        assertEquals("PRIMARY",value("SELECT type FROM core.relationship"));assertTrue(db.getAutoCommit());
    }
    @Test void completedRetryAndCrossDateReplayDoNotDoublePost() throws Exception {
        fixture();tx(day,1,10,"CR",100);var first=process(day);assertEquals(first,process(day));
        var next=day.plusDays(1);receipt(next);tx(next,1,10,"CR",100);tx(next,2,11,"DR",20);
        assertEquals(1L,process(next).get("transaction"));
        assertEquals("80",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("2",value("SELECT count(*) FROM core.batch_run"));
    }
    @Test void effectiveDateControlsEligibilityIndependentlyOfBatchDateAndReplayIdentity() throws Exception {
        fixture();tx(day,1,10,"CR",100);
        sql("UPDATE core_ingest.account SET start_date='2026-10-10'");
        sql("UPDATE core_ingest.transaction SET effective_date='2026-10-10'");
        assertEquals(1L,process(day).get("transaction"));
        assertEquals("2026-10-10",value("SELECT effective_date FROM core.transaction"));
        assertEquals("2026-10-09",value("SELECT batch_date FROM core.transaction"));
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        var next=day.plusDays(3);receipt(next);tx(next,1,10,"CR",100);
        sql("UPDATE core_ingest.transaction SET effective_date='2026-10-11' WHERE batch_date='"+next+"'");
        assertTrue(assertThrows(SQLException.class,()->process(next)).getMessage().contains("Conflicting transaction"));
        sql("UPDATE core_ingest.transaction SET effective_date='2026-10-10' WHERE batch_date='"+next+"'");
        assertEquals(0L,process(next).get("transaction"));
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
    }
    @Test void rejectsEffectiveDateBeforeStartEvenWhenBatchDateIsLater() throws Exception {
        fixture();tx(day,1,10,"CR",100);
        sql("UPDATE core_ingest.account SET start_date='2026-10-08'");
        sql("UPDATE core_ingest.transaction SET effective_date='2026-10-07'");
        assertTrue(assertThrows(SQLException.class,()->process(day)).getMessage().contains("Ineligible transaction"));
        assertEquals("0",value("SELECT count(*) FROM core.transaction"));
        assertEquals("0",value("SELECT count(*) FROM core.balance"));
        sql("UPDATE core_ingest.transaction SET effective_date='2026-10-08'");
        assertEquals(1L,process(day).get("transaction"));
    }
    @Test void upsertsReferencesWithoutResettingBalanceOrDeletingOmittedRows() throws Exception {
        fixture();tx(day,1,10,"CR",100);process(day);
        var next=day.plusDays(1);receipt(next);account(next,2);
        sql("UPDATE core_ingest.account SET account_type='SAVINGS' WHERE batch_date='"+next+"'");
        sql("INSERT INTO core_ingest.customer SELECT '"+next+"',source_file,source_line,customer_id,'Jo',last_name,address_line1,city,province,postal_code,country FROM core_ingest.customer");
        sql("INSERT INTO core_ingest.relationship VALUES ('"+next+"','r.dat',1,2,1,'SECONDARY')");
        process(next);
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("SAVINGS",value("SELECT account_type FROM core.account"));
        assertEquals("Jo",value("SELECT first_name FROM core.customer"));
        assertEquals("SECONDARY",value("SELECT type FROM core.relationship"));
        receipt(next.plusDays(1));process(next.plusDays(1));
        assertEquals("1",value("SELECT count(*) FROM core.account"));
    }
    @Test void conflictingReplayFailsWholeBatchAndCanBeCorrectedAndRetried() throws Exception {
        fixture();tx(day,1,10,"CR",100);process(day);
        var next=day.plusDays(1);receipt(next);account(next,3);tx(next,1,10,"DR",100);
        assertTrue(assertThrows(SQLException.class,()->process(next)).getMessage().contains("Conflicting transaction"));
        assertEquals("1",value("SELECT count(*) FROM core.account"));
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("0",value("SELECT account_count FROM core.batch_run WHERE status='FAILED'"));
        sql("UPDATE core_ingest.transaction SET type='CR' WHERE batch_date='"+next+"'");
        assertEquals(0L,process(next).get("transaction"));
        assertEquals("2",value("SELECT count(*) FROM core.batch_run WHERE batch_date='"+next+"'"));
    }
    @Test void rejectsDuplicatesForEveryRecordType() throws Exception {
        for(String table:new String[]{"customer","account","relationship","transaction"}) {
            clean();fixture();tx(day,1,10,"CR",10);
            String fields=switch(table) {
                case "customer" -> "customer_id,first_name,last_name,address_line1,city,province,postal_code,country";
                case "account" -> "account_id,start_date,end_date,account_type";
                case "relationship" -> "account_id,customer_id,type";
                default -> "transaction_id,account_id,type,amount,effective_date";
            };
            sql("INSERT INTO core_ingest."+table+" SELECT batch_date,source_file,99,"+fields+" FROM core_ingest."+table);
            assertTrue(assertThrows(SQLException.class,()->process(day)).getMessage().contains("Duplicate "+table));
            assertEquals("0",value("SELECT count(*) FROM core.account"));
        }
    }
    @Test void rejectsMissingReferencesAndInvalidDates() throws Exception {
        fixture();sql("UPDATE core_ingest.relationship SET customer_id=999");
        assertTrue(assertThrows(SQLException.class,()->process(day)).getMessage().contains("Missing customer"));
        sql("UPDATE core_ingest.relationship SET customer_id=1,account_id=999");
        assertTrue(assertThrows(SQLException.class,()->process(day)).getMessage().contains("Missing account"));
        sql("UPDATE core_ingest.relationship SET account_id=2");
        sql("UPDATE core_ingest.account SET end_date='2025-01-01'");
        assertTrue(assertThrows(SQLException.class,()->process(day)).getMessage().contains("precedes"));
    }
    @Test void rejectsEndDatedAndFutureAccountsAndRollsBackUpserts() throws Exception {
        fixture();tx(day,1,10,"CR",1);
        sql("UPDATE core_ingest.account SET end_date='2027-01-01'");
        assertThrows(SQLException.class,()->process(day));
        assertEquals("0",value("SELECT count(*) FROM core.customer"));
        sql("UPDATE core_ingest.account SET end_date=NULL,start_date='2027-01-01'");
        assertThrows(SQLException.class,()->process(day));
        assertEquals("0",value("SELECT count(*) FROM core.balance"));
    }
    @Test void replayOnNowClosedAccountIsHarmless() throws Exception {
        fixture();tx(day,1,10,"CR",1);process(day);
        var next=day.plusDays(1);receipt(next);account(next,2);tx(next,1,10,"CR",1);
        sql("UPDATE core_ingest.account SET end_date='2027-01-01' WHERE batch_date='"+next+"'");
        assertEquals(0L,process(next).get("transaction"));
        assertEquals("1",value("SELECT balance FROM core.balance"));
    }
    @Test void overflowRollsBackLedgerAndReferences() throws Exception {
        fixture();tx(day,1,10,"CR",Long.MAX_VALUE);tx(day,2,11,"CR",1);
        assertThrows(SQLException.class,()->process(day));
        assertEquals("0",value("SELECT count(*) FROM core.transaction"));
        assertEquals("0",value("SELECT count(*) FROM core.account"));
        assertEquals("FAILED",value("SELECT status FROM core.batch_run"));
        assertTrue(db.getAutoCommit());
    }
    @Test void aggregationAllowsNetWithinRangeDespiteLargeIntermediateTotal() throws Exception {
        fixture();tx(day,1,10,"CR",Long.MAX_VALUE);tx(day,2,11,"CR",Long.MAX_VALUE);tx(day,3,12,"DR",Long.MAX_VALUE);
        process(day);assertEquals(Long.toString(Long.MAX_VALUE),value("SELECT balance FROM core.balance"));
    }
    @Test void requiresReceiptButAcceptsRealEmptyIngestionAndProtectsCompletedDate() throws Exception {
        assertThrows(SQLException.class,()->process(day));
        for(String table:new String[]{"customer","account","relationship","transaction"})
            Files.writeString(input.resolve(table+"_20261009.dat"),"");
        new BatchIngestor().ingest(db,input,day);
        assertEquals(0L,process(day).get("transaction"));
        assertThrows(IllegalStateException.class,()->new BatchIngestor().ingest(db,input,day));
        assertEquals("1",value("SELECT count(*) FROM core_ingest.batch_receipt"));
    }
    @Test void blocksOlderDatesAndUnfinishedAttempts() throws Exception {
        receipt(day);process(day);receipt(day.minusDays(1));
        assertThrows(SQLException.class,()->process(day.minusDays(1)));
        sql("INSERT INTO core.batch_run(batch_date) VALUES ('2026-10-10')");
        assertTrue(assertThrows(SQLException.class,()->process(day.plusDays(1))).getMessage().contains("unfinished"));
        receipt(day.plusDays(2));
        assertTrue(assertThrows(SQLException.class,()->process(day.plusDays(2))).getMessage().contains("unfinished"));
    }
    @Test void concurrentProcessorsPostOnce() throws Exception {
        fixture();tx(day,1,10,"CR",100);
        try(var pool=Executors.newFixedThreadPool(2)) {
            var start=new CountDownLatch(1);
            Callable<java.util.Map<String,Long>> action=()-> { start.await();try(var c=connect()) { return new BatchProcessor().process(c,day); } };
            var a=pool.submit(action);var b=pool.submit(action);start.countDown();
            assertEquals(a.get(10,TimeUnit.SECONDS),b.get(10,TimeUnit.SECONDS));
        }
        assertEquals("100",value("SELECT balance FROM core.balance"));
        assertEquals("1",value("SELECT count(*) FROM core.batch_run"));
    }
    private Path output(LocalDate date) throws Exception {
        return new corebank.output.BalanceExporter().export(db, input.resolve("output"), date);
    }
    @Test void snapshotSelectsNewAndActiveAccountsAndExcludesReplaysAndReferenceOnlyUpdates() throws Exception {
        fixture(); account(day,3); tx(day,1,10,"CR",100); process(day);
        assertEquals("2",value("SELECT record_count FROM core_output.batch_snapshot"));
        assertEquals("0",value("SELECT balance FROM core_output.balance WHERE account_id=3"));
        var next=day.plusDays(1); receipt(next); account(next,3); account(next,4);
        tx(next,1,11,"CR",20); tx(next,2,12,"DR",20); process(next);
        assertEquals("2,4",value("SELECT string_agg(account_id::text,',' ORDER BY account_id) FROM core_output.balance WHERE batch_date='"+next+"'"));
        assertEquals("100",value("SELECT balance FROM core_output.balance WHERE batch_date='"+next+"' AND account_id=2"));
        var third=next.plusDays(1);receipt(third);tx(third,1,10,"CR",100);process(third);
        assertEquals("0",value("SELECT record_count FROM core_output.batch_snapshot WHERE batch_date='"+third+"'"));
        var fourth=third.plusDays(1);receipt(fourth);tx(fourth,1,13,"CR",0);process(fourth);
        assertEquals("1",value("SELECT record_count FROM core_output.batch_snapshot WHERE batch_date='"+fourth+"'"));
    }
    @Test void historicalOutputIsStableSortedAndIdempotent() throws Exception {
        fixture();account(day,1);tx(day,1,10,"DR",50);process(day);
        String expected="                  1                   0\n                  2                 -50\n";
        Path file=output(day);assertEquals(expected,Files.readString(file));
        var modified=Files.getLastModifiedTime(file);assertEquals(file,output(day));
        assertEquals(modified,Files.getLastModifiedTime(file));
        var next=day.plusDays(1);receipt(next);tx(next,1,11,"CR",200);process(next);
        Files.delete(file);assertEquals(expected,Files.readString(output(day)));
        assertEquals(2L,process(day).get("account"));
        assertEquals("2",value("SELECT count(*) FROM core_output.balance WHERE batch_date='"+day+"'"));
        assertEquals("150",value("SELECT balance FROM core.balance WHERE account_id=2"));
    }
    @Test void outputSupportsLongExtremesAndZeroByteEmptySnapshots() throws Exception {
        fixture();account(day,Long.MAX_VALUE);tx(day,1,10,"DR",Long.MAX_VALUE);tx(day,2,11,"DR",1);
        sql("INSERT INTO core_ingest.transaction VALUES ('"+day+"','t.dat',3,12,"+Long.MAX_VALUE+",'CR',"+Long.MAX_VALUE+",'2026-10-09')");
        process(day);
        String text=Files.readString(output(day));
        assertEquals("                  2-9223372036854775808\n9223372036854775807 9223372036854775807\n",text);
        for(String line:text.split("\n")) assertEquals(39,line.length());
        var next=day.plusDays(1);receipt(next);process(next);assertEquals(0,Files.size(output(next)));
    }
    @Test void rejectsMissingFailedRunningAndLegacySnapshots() throws Exception {
        assertThrows(SQLException.class,()->output(day));
        sql("INSERT INTO core.batch_run(batch_date) VALUES ('"+day+"')");
        assertThrows(SQLException.class,()->output(day));
        sql("UPDATE core.batch_run SET status='FAILED',completed_at=clock_timestamp()");
        assertThrows(SQLException.class,()->output(day));
        sql("UPDATE core.batch_run SET status='COMPLETED'");
        assertThrows(SQLException.class,()->output(day));
        assertFalse(Files.exists(input.resolve("output")));assertTrue(db.getAutoCommit());
    }
    @Test void failedSnapshotRollsBackPostingAndRetrySucceeds() throws Exception {
        fixture();tx(day,1,10,"CR",100);
        sql("ALTER TABLE core_output.balance ADD CONSTRAINT output_test_failure CHECK(balance<0)");
        try {
            assertThrows(SQLException.class,()->process(day));
            assertEquals("0",value("SELECT count(*) FROM core_output.batch_snapshot"));
            assertEquals("0",value("SELECT count(*) FROM core.transaction"));
            assertEquals("0",value("SELECT count(*) FROM core.account"));
            assertEquals("FAILED",value("SELECT status FROM core.batch_run"));
        } finally { sql("ALTER TABLE core_output.balance DROP CONSTRAINT output_test_failure"); }
        process(day);assertTrue(Files.exists(output(day)));
        assertThrows(SQLException.class,()->sql("UPDATE core_output.balance SET balance=1"));
        assertThrows(SQLException.class,()->sql("DELETE FROM core_output.batch_snapshot"));
    }
    @Test void fileFailureCanRetryWithoutRepostingAndConflictingFileIsPreserved() throws Exception {
        fixture();tx(day,1,10,"CR",100);process(day);
        Path directory=input.resolve("output");Files.writeString(directory,"obstruction");
        assertThrows(java.io.IOException.class,()->output(day));
        assertEquals("COMPLETED",value("SELECT status FROM core.batch_run"));
        Files.delete(directory);Path file=output(day);Files.writeString(file,"conflict");
        assertThrows(java.io.IOException.class,()->output(day));assertEquals("conflict",Files.readString(file));
        try(var files=Files.list(directory)) { assertEquals(1,files.count()); }
        Files.delete(file);output(day);assertEquals("1",value("SELECT count(*) FROM core.transaction"));
    }
    @Test void concurrentExportsPublishOneCompleteFile() throws Exception {
        fixture();tx(day,1,10,"CR",100);process(day);
        try(var pool=Executors.newFixedThreadPool(2)) {
            var start=new CountDownLatch(1);
            Callable<Path> action=()-> { start.await();try(var c=connect()) { return new corebank.output.BalanceExporter().export(c,input.resolve("output"),day); } };
            var a=pool.submit(action);var b=pool.submit(action);start.countDown();
            assertEquals(a.get(10,TimeUnit.SECONDS),b.get(10,TimeUnit.SECONDS));
        }
        assertEquals("                  2                 100\n",Files.readString(output(day)));
    }

}
