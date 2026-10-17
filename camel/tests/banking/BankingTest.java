package banking;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

public class BankingTest {
    @TempDir Path root;
    Settings settings;
    Store store;
    FilesBridge files;
    static final String CUSTOMER="{\"customerId\":1,\"firstName\":\"José\",\"lastName\":\"Doe\",\"addressLine1\":\"123 Main\",\"city\":\"Toronto\",\"province\":\"ON\",\"postalCode\":\"A1B 2C3\",\"country\":\"Canada\"}";
    static final String ACCOUNT="{\"accountId\":2,\"startDate\":\"2026-10-09\",\"endDate\":null,\"accountType\":\"CHECKING\"}";
    static final String RELATIONSHIP="{\"accountId\":2,\"customerId\":1,\"type\":\"PRIMARY\"}";
    static final String TRANSACTION="{\"transactionId\":3,\"accountId\":2,\"type\":\"CR\",\"amount\":12345}";
    static final LocalDate DAY=LocalDate.of(2026,10,9);
    @BeforeEach void setup() throws Exception {
        settings=new Settings(System.getenv().getOrDefault("CAMEL_TEST_BROKERS","localhost:39092"),"test-"+UUID.randomUUID(),
            System.getenv("COREBANK_TEST_DB_URL"),"postgres",System.getenv("COREBANK_TEST_DB_PASSWORD"),root.resolve("input"),root.resolve("output"),root.resolve("error"));
        store=new Store(settings); files=new FilesBridge(store,settings);
        try (var db=settings.connect(); var s=db.createStatement()) {
            s.execute("TRUNCATE camel_ingest.record,camel_ingest.receipt,camel_ingest.batch,camel_ingest.publication");
            s.execute("UPDATE camel_ingest.clock SET control_date=NULL");
        }
        Files.createDirectories(settings.output());
    }
    Store.Source source(String topic,long offset,String key,String value) { return new Store.Source(topic,0,offset,key,value); }
    LocalDate date(long offset,String value) throws Exception { return store.accept(source(Topics.DATE,offset,"clock",value)); }
    void initial() throws Exception { date(0,"2026-10-08"); }
    void stageAll() throws Exception {
        store.accept(source("input.customer",0,"1",CUSTOMER)); store.accept(source("input.account",0,"2",ACCOUNT));
        store.accept(source("input.relationship",0,"2:1",RELATIONSHIP)); store.accept(source("input.transaction",0,"3",TRANSACTION));
    }
    long scalar(String sql) throws Exception { try(var db=settings.connect();var s=db.createStatement();var r=s.executeQuery(sql)) { r.next();return r.getLong(1); } }
    @Test void requiresInitialDateAndPersistsClock() throws Exception {
        assertThrows(Store.AwaitingClock.class,() -> store.accept(source("input.account",0,"2",ACCOUNT)));
        assertEquals(0,scalar("SELECT count(*) FROM camel_ingest.receipt"));
        assertEquals(DAY,date(0,"2026-10-08"));
        assertEquals(DAY,new Store(settings).accept(source("input.account",0,"2",ACCOUNT)));
    }
    @Test void datesRepeatAdvanceRejectGapsAndReplay() throws Exception {
        initial(); assertEquals(DAY,date(1,"2026-10-08"));
        assertThrows(IllegalArgumentException.class,() -> date(2,"2026-10-10"));
        assertThrows(IllegalArgumentException.class,() -> date(2,"2026-10-07"));
        assertEquals(DAY.plusDays(1),date(2,"2026-10-09"));
        assertEquals(DAY,date(0,"2026-10-08")); // old source replay is recognized before backward-date validation
        assertEquals(1,scalar("SELECT count(*) FROM camel_ingest.batch WHERE closed"));
        assertThrows(java.time.DateTimeException.class,() -> date(3,"2026-02-30"));
    }
    @Test void redeliveryKeepsOriginalDateAndDoesNotRevertNewerUpsert() throws Exception {
        initial(); store.accept(source("input.customer",0,"1",CUSTOMER));
        store.accept(source("input.customer",1,"1",CUSTOMER.replace("José","Jane")));
        store.accept(source("input.customer",0,"1",CUSTOMER));
        date(1,"2026-10-09");
        assertEquals(DAY,store.accept(source("input.customer",0,"1",CUSTOMER)));
        assertEquals(1,scalar("SELECT count(*) FROM camel_ingest.customer"));
        assertEquals(1,scalar("SELECT count(*) FROM camel_ingest.customer WHERE fixed_line LIKE '%Jane%'"));
        assertThrows(IllegalArgumentException.class,() -> store.accept(source("input.customer",0,"1",CUSTOMER.replace("José","Bob"))));
    }
    @Test void conflictingTransactionsAndKeysRollback() throws Exception {
        initial(); store.accept(source("input.transaction",0,"3",TRANSACTION));
        assertThrows(IllegalArgumentException.class,() -> store.accept(source("input.transaction",1,"3",TRANSACTION.replace("12345","999"))));
        assertThrows(IllegalArgumentException.class,() -> store.accept(source("input.account",0,"wrong",ACCOUNT)));
        assertEquals(2,scalar("SELECT count(*) FROM camel_ingest.receipt"));
    }
    @Test void strictJsonUnicodeAndNumericBoundaries() throws Exception {
        assertEquals(88,Records.decode("customer",CUSTOMER).line().codePointCount(0,Records.decode("customer",CUSTOMER).line().length()));
        assertEquals(Records.decode("customer",CUSTOMER),Records.decode("customer",CUSTOMER.replace("José","Jose\u0301")));
        assertThrows(Exception.class,() -> Records.decode("transaction",TRANSACTION.replace("12345","9223372036854775808")));
        assertThrows(Exception.class,() -> Records.decode("transaction",TRANSACTION.replace("12345","1.5")));
        assertThrows(Exception.class,() -> Records.decode("transaction",TRANSACTION.replace("12345","-1")));
        assertThrows(Exception.class,() -> Records.decode("transaction",TRANSACTION+" {}"));
        assertThrows(Exception.class,() -> Records.decode("customer",CUSTOMER.replace("José","longerthan10letters")));
        assertThrows(Exception.class,() -> Records.decode("customer",CUSTOMER.replace("José","Jo\\nse")));
        assertThrows(Exception.class,() -> Records.decode("account",ACCOUNT.replace("2026-10-09","2026-02-30")));
        assertThrows(Exception.class,() -> Records.decode("account",ACCOUNT.replace("\"accountId\":2","\"accountId\":0")));
        assertThrows(Exception.class,() -> Records.decode("account",ACCOUNT.replace("\"accountId\":2","\"accountId\":2,\"accountId\":3")));
    }
    @Test void closedExtractionAndCoreRoundTrip() throws Exception {
        initial(); stageAll(); assertFalse(files.extractNextClosedBatchIfAvailable()); date(1,"2026-10-09");
        assertTrue(files.extractNextClosedBatchIfAvailable()); assertFalse(files.extractNextClosedBatchIfAvailable());
        assertTrue(Files.exists(settings.input().resolve("batch_20261009.trg")));
        try (var db=settings.connect(); var s=db.createStatement()) {
            // Disposable database only. Existing core integration tests use the same reset policy.
            s.execute("TRUNCATE core_output.balance,core_output.batch_snapshot,core.transaction,core.relationship,core.balance,core.account,core.customer,core.batch_run,core_ingest.customer,core_ingest.account,core_ingest.relationship,core_ingest.transaction,core_ingest.batch_receipt CASCADE");
            new corebank.ingest.BatchIngestor().ingest(db,settings.input(),DAY);
            new corebank.processing.BatchProcessor().process(db,DAY);
            new corebank.output.BalanceExporter().export(db,settings.output(),DAY);
        }
        var sent=new ArrayList<String>();
        assertEquals(1,files.publishPendingBalanceFiles((k,v) -> sent.add(k+"="+v)));
        assertEquals(1,sent.size()); assertTrue(sent.getFirst().startsWith("2026-10-09:2="));
        assertEquals(12345,Records.JSON.readTree(sent.getFirst().split("=",2)[1]).get("balance").longValue());
        assertEquals(0,files.publishPendingBalanceFiles((k,v) -> fail("Already published")));
    }
    @Test void extractionConflictDoesNotSignalReadyAndRecovers() throws Exception {
        initial(); date(1,"2026-10-09"); Files.createDirectories(settings.input());
        Path conflict=settings.input().resolve("account_20261009.dat"); Files.writeString(conflict,"conflict");
        assertThrows(Exception.class,files::extractNextClosedBatchIfAvailable);
        assertFalse(Files.exists(settings.input().resolve("batch_20261009.trg")));
        assertEquals(0,scalar("SELECT count(*) FROM camel_ingest.batch WHERE extracted"));
        Files.delete(conflict); assertTrue(files.extractNextClosedBatchIfAvailable());
        for(String kind:Topics.KINDS) assertEquals(0,Files.size(settings.input().resolve(kind+"_20261009.dat")));
        // Simulate the trigger being consumed just before a failed DB commit.
        Files.move(settings.input().resolve("batch_20261009.trg"),settings.output().resolve("batch_20261009.COMPLETE.trg"));
        try(var db=settings.connect();var s=db.createStatement()) { s.execute("UPDATE camel_ingest.batch SET extracted=FALSE WHERE closed"); }
        assertTrue(files.extractNextClosedBatchIfAvailable()); assertFalse(Files.exists(settings.input().resolve("batch_20261009.trg")));
    }
    @Test void publicationFailureRedeliversStableKeysAndRejectsMutation() throws Exception {
        Path file=settings.output().resolve("balance_20261009.dat");
        Files.writeString(file,String.format("%19d%20d\n%19d%20d\n",1,Long.MIN_VALUE,2,Long.MAX_VALUE));
        var keys=new ArrayList<String>();
        assertThrows(Exception.class,() -> files.publishPendingBalanceFiles((k,v) -> { keys.add(k); if(keys.size()==2) throw new Exception("broker unavailable"); }));
        assertEquals(0,scalar("SELECT count(*) FROM camel_ingest.publication"));
        assertEquals(1,files.publishPendingBalanceFiles((k,v) -> keys.add(k)));
        assertEquals(keys.subList(0,2),keys.subList(2,4));
        Files.writeString(file,""); assertThrows(Exception.class,() -> files.publishPendingBalanceFiles((k,v) -> fail("Changed file")));
    }
    @Test void validatesWholeBalanceFileBeforeAnySendAndHandlesEmptyFile() throws Exception {
        Path file=settings.output().resolve("balance_20261009.dat");
        Files.writeString(file,String.format("%19d%20d\nbad\n",1,2));
        assertThrows(Exception.class,() -> files.publishPendingBalanceFiles((k,v) -> fail("Must validate before publishing")));
        Files.writeString(file,""); assertEquals(1,files.publishPendingBalanceFiles((k,v) -> fail("Empty file")));
        assertEquals(0,files.publishPendingBalanceFiles((k,v) -> fail("Empty file replay")));
    }
    @Test void stagingAndDateTransitionShareADatabaseBoundary() throws Exception {
        initial();
        try(var db=settings.connect()) {
            db.setAutoCommit(false);
            try(var s=db.createStatement()) { s.execute("SELECT * FROM camel_ingest.clock FOR UPDATE"); }
            try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
                var pending=pool.submit(() -> date(1,"2026-10-09"));
                Thread.sleep(150); assertFalse(pending.isDone());
                db.commit(); assertEquals(DAY.plusDays(1),pending.get(5,TimeUnit.SECONDS));
            }
        }
        assertEquals(DAY.plusDays(1),store.accept(source("input.account",0,"2",ACCOUNT)));
    }
    @Test void realKafkaRoutesCommitOnlyAfterStagingAndPublishFiles() throws Exception {
        Assumptions.assumeTrue(System.getenv("CAMEL_TEST_BROKERS") != null,"Set CAMEL_TEST_BROKERS to the disposable broker");
        Topics.provision(settings.brokers()); Topics.provision(settings.brokers()); // idempotent
        try(var producer=new KafkaProducer<String,String>(Map.of("bootstrap.servers",settings.brokers(),"acks","all"),new StringSerializer(),new StringSerializer());
            var consumer=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",settings.brokers(),"group.id","assert-"+UUID.randomUUID(),"auto.offset.reset","earliest"),new StringDeserializer(),new StringDeserializer());
            var context=new DefaultCamelContext()) {
            // Test broker is reset between suite runs; no production topics are touched.
            producer.send(new ProducerRecord<>("input.account","2",ACCOUNT)).get();
            context.addRoutes(new BankingRoutes(settings)); context.start();
            Thread.sleep(2000);
            assertEquals(0,scalar("SELECT count(*) FROM camel_ingest.receipt"));
            producer.send(new ProducerRecord<>(Topics.DATE,"clock","2026-10-08")).get();
            await(() -> scalar("SELECT count(*) FROM camel_ingest.account")==1);
            producer.send(new ProducerRecord<>("input.customer","1",CUSTOMER)).get();
            producer.send(new ProducerRecord<>("input.relationship","2:1",RELATIONSHIP)).get();
            producer.send(new ProducerRecord<>("input.transaction","3",TRANSACTION)).get();
            await(() -> scalar("SELECT count(*) FROM camel_ingest.record")==4);
            producer.send(new ProducerRecord<>(Topics.DATE,"clock","2026-10-09")).get();
            await(() -> Files.exists(settings.input().resolve("batch_20261009.trg")));
            Files.writeString(settings.output().resolve("balance_20261009.dat"),String.format("%19d%20d\n",2,12345));
            consumer.subscribe(List.of(Topics.BALANCE));
            var received=new ArrayList<ConsumerRecord<String,String>>();
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
            while(received.isEmpty() && System.nanoTime()<deadline) consumer.poll(Duration.ofMillis(500)).forEach(received::add);
            assertFalse(received.isEmpty()); assertEquals("2026-10-09:2",received.getFirst().key());
            assertEquals(12345,Records.JSON.readTree(received.getFirst().value()).get("balance").longValue());
            await(() -> scalar("SELECT count(*) FROM camel_ingest.publication")==1);
            context.stop();
            // Fresh context and the same group must not restage consumed input or republish the file.
            try(var restarted=new DefaultCamelContext()) {
                restarted.addRoutes(new BankingRoutes(settings)); restarted.start(); Thread.sleep(2000);
                assertEquals(4,scalar("SELECT count(*) FROM camel_ingest.record"));
                assertEquals(6,scalar("SELECT count(*) FROM camel_ingest.receipt"));
                assertTrue(consumer.poll(Duration.ofSeconds(2)).isEmpty());
            }
        }
    }
    @FunctionalInterface interface Check { boolean get() throws Exception; }
    static void await(Check check) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(!check.get()) { if(System.nanoTime()>end) fail("Timed out"); Thread.sleep(100); }
    }
}
