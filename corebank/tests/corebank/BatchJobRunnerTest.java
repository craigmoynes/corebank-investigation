package corebank;

import corebank.batch.BatchJobRunner;
import corebank.ingest.BatchIngestor;
import corebank.processing.BatchProcessor;
import java.sql.*;
import java.time.LocalDate;
import java.nio.file.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class BatchJobRunnerTest {
    @TempDir Path root;
    Connection db;
    Config config;
    LocalDate day = LocalDate.of(2026,10,9);
    @BeforeEach void setup() throws Exception {
        var database = new Config.Database(System.getenv("COREBANK_TEST_DB_URL"),
            System.getenv().getOrDefault("COREBANK_TEST_DB_USER", "postgres"), System.getenv("COREBANK_TEST_DB_PASSWORD"));
        db = DriverManager.getConnection(database.url(), database.username(), database.password());
        config = new Config(Files.createDirectory(root.resolve("input")), root.resolve("output"), root.resolve("error"), database);
        clean();
        for (String type : new String[]{"customer","account","relationship","transaction"}) Files.writeString(file(type), "");
    }
    @AfterEach void teardown() throws Exception { if (db != null) { clean(); db.close(); } }
    void clean() throws Exception {
        sql("TRUNCATE core_batch.batch_job_instance CASCADE");
        sql("TRUNCATE core_output.balance,core_output.batch_snapshot,core.transaction,core.relationship,core.balance,core.account,core.customer,core.batch_run,core_ingest.customer,core_ingest.account,core_ingest.relationship,core_ingest.transaction,core_ingest.batch_receipt");
    }
    void sql(String text) throws Exception { try(var s=db.createStatement()) { s.execute(text); } }
    String value(String text) throws Exception { try(var s=db.createStatement();var rs=s.executeQuery(text)) { assertTrue(rs.next());return rs.getString(1); } }
    Path file(String type) { return config.inputDirectory().resolve(type+"_20261009.dat"); }
    void fixture(long account) throws Exception {
        Files.writeString(file("account"), String.format(java.util.Locale.ROOT,"%19d%s%10s%-8s\n", 2, day, "", "CHECKING"));
        Files.writeString(file("transaction"), String.format(java.util.Locale.ROOT,"%19d%19d%-6s%20d2026-10-09\n", 10, account, "CR", 100));
    }
    void run() throws Exception { new BatchJobRunner().run(config,day); }
    @Test void completesAndDuplicateDateIsNoOp() throws Exception {
        fixture(2); run(); run();
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("1",value("SELECT count(*) FROM core_batch.batch_job_execution"));
        assertEquals("3",value("SELECT count(*) FROM core_batch.batch_step_execution WHERE status='COMPLETED'"));
        assertEquals(String.format(java.util.Locale.ROOT,"%19d%20d\n",2,100), Files.readString(config.outputDirectory().resolve("balance_20261009.dat")));
    }
    @Test void missingInputCanBeFixedAndEmptyBatchCompletes() throws Exception {
        Files.delete(file("customer")); assertThrows(Exception.class,this::run);
        assertEquals("0",value("SELECT count(*) FROM core.batch_run"));
        Files.writeString(file("customer"), ""); run();
        assertEquals(0,Files.size(config.outputDirectory().resolve("balance_20261009.dat")));
    }
    @Test void failedPostingReloadsCorrectedFiles() throws Exception {
        fixture(999); assertThrows(Exception.class,this::run);
        assertEquals("0",value("SELECT count(*) FROM core.transaction"));
        fixture(2); run();
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("2",value("SELECT count(*) FROM core.batch_run"));
    }
    @Test void outputRetryDoesNotNeedInputsOrRepost() throws Exception {
        fixture(2); Files.writeString(config.outputDirectory(),"blocks directory creation");
        assertThrows(Exception.class,this::run);
        assertEquals("COMPLETED",value("SELECT status FROM core.batch_run"));
        for (String type : new String[]{"customer","account","relationship","transaction"}) Files.delete(file(type));
        Files.delete(config.outputDirectory()); run();
        assertEquals("1",value("SELECT count(*) FROM core.batch_run"));
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
        assertEquals("1",value("SELECT count(*) FROM core_batch.batch_step_execution WHERE step_name='process'"));
    }
    @Test void reconcilesCommittedPostingWithFailedStepMetadata() throws Exception {
        fixture(999); assertThrows(Exception.class,this::run);
        // Model recovery after a business commit whose step completion was not recorded.
        fixture(2); new BatchIngestor().ingest(db,config.inputDirectory(),day); new BatchProcessor().process(db,day);
        Files.delete(file("account")); run();
        assertEquals("1",value("SELECT count(*) FROM core.transaction"));
        assertEquals("100",value("SELECT balance FROM core.balance WHERE account_id=2"));
    }
    @Test void earlierFailedJobBlocksLaterDate() throws Exception {
        Files.delete(file("account")); assertThrows(Exception.class,this::run);
        var e=assertThrows(IllegalStateException.class,()->new BatchJobRunner().run(config,day.plusDays(1)));
        assertTrue(e.getMessage().contains("2026-10-09"));
        assertEquals("1",value("SELECT count(*) FROM core_batch.batch_job_instance"));
    }
    @Test void activeGuardPreventsConcurrentLaunchAndReleasesAfterwards() throws Exception {
        sql("SELECT pg_advisory_lock("+BatchLocks.JOB_LOCK_NAMESPACE+",0)");
        assertThrows(IllegalStateException.class,this::run);
        sql("SELECT pg_advisory_unlock("+BatchLocks.JOB_LOCK_NAMESPACE+",0)");
        run();
    }
    @Test void abandonedExecutionRequiresOperatorReview() throws Exception {
        Files.delete(file("account")); assertThrows(Exception.class,this::run);
        sql("UPDATE core_batch.batch_job_execution SET status='STARTED'");
        assertTrue(assertThrows(IllegalStateException.class,this::run).getMessage().contains("STARTED"));
    }
    @Test void unfinishedBusinessAttemptIsNotAutomaticallyRecovered() throws Exception {
        sql("INSERT INTO core.batch_run(batch_date) VALUES ('2026-10-09')");
        assertThrows(Exception.class,this::run);
        assertEquals("RUNNING",value("SELECT status FROM core.batch_run"));
    }
}
