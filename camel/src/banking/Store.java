package banking;

import java.sql.*;
import java.time.LocalDate;

/** Each callback owns a fresh JDBC transaction. The clock row serializes assignment and closure. */
public final class Store {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(Store.class);
    private record Accepted(LocalDate date, LocalDate previousDate, boolean dateChanged, boolean duplicate) {}
    public static final class AwaitingClock extends IllegalStateException {
        public AwaitingClock() { super("Waiting for the initial system.batch-date record"); }
    }
    public record Source(String topic, int partition, long offset, String key, String payload) {
        public Source {
            if (partition != 0 || offset < 0) throw new IllegalArgumentException("Expected partition zero and nonnegative offset");
            if (payload == null) throw new IllegalArgumentException("Tombstones are not supported");
        }
    }
    @FunctionalInterface public interface Work<T> { T run(Connection db) throws Exception; }
    @FunctionalInterface private interface Connections { Connection open() throws SQLException; }
    private final Connections connections;
    public Store(Settings settings) { connections=settings::connect; }
    public Store(javax.sql.DataSource dataSource) { connections=dataSource::getConnection; }
    public <T> T inTransaction(Work<T> work) throws Exception {
        try (Connection db=connections.open()) {
            db.setAutoCommit(false);
            try { T result=work.run(db); db.commit(); return result; }
            catch (Exception e) { try { db.rollback(); } catch (Exception rollback) { e.addSuppressed(rollback); } throw e; }
        }
    }
    public LocalDate accept(Source source) throws Exception {
        Accepted accepted=inTransaction(db -> {
            LocalDate control;
            try (var s=db.createStatement(); var r=s.executeQuery("SELECT active_date FROM camel_ingest.clock WHERE singleton FOR UPDATE")) {
                if (!r.next()) throw new IllegalStateException("Missing clock singleton");
                control=r.getObject(1,LocalDate.class);
            }
            try (var s=db.prepareStatement("SELECT batch_date,payload FROM camel_ingest.receipt WHERE topic=? AND partition_id=? AND source_offset=?")) {
                identity(s,source);
                try (var r=s.executeQuery()) {
                    if (r.next()) {
                        if (!source.payload().equals(r.getString(2))) throw new IllegalArgumentException("Source identity reused with different payload");
                        return new Accepted(r.getObject(1,LocalDate.class),control,false,true);
                    }
                }
            }
            if (Topics.DATE.equals(source.topic())) {
                if (!"clock".equals(source.key())) throw new IllegalArgumentException("system.batch-date key must be clock");
                LocalDate next=Records.date(source.payload());
                LocalDate active=next;
                if (control != null && next.isBefore(control))
                    throw new IllegalArgumentException("Date must repeat or advance to a later business date; current="+control+", received="+next);
                if (control == null || !control.equals(next)) {
                    if (control != null) execute(db,"UPDATE camel_ingest.batch SET closed=TRUE WHERE batch_date=?",control);
                    execute(db,"INSERT INTO camel_ingest.batch(batch_date) VALUES (?) ON CONFLICT DO NOTHING",active);
                    execute(db,"UPDATE camel_ingest.clock SET active_date=?,updated_at=now() WHERE singleton",next);
                }
                receipt(db,source,active);
                return new Accepted(active,control,control == null || !control.equals(next),false);
            }
            if (control == null) throw new AwaitingClock();
            String kind=source.topic().startsWith("input.") ? source.topic().substring(6) : "";
            Records.Row row=Records.decode(kind,source.payload());
            if (!row.key().equals(source.key())) throw new IllegalArgumentException("Kafka key must match business key "+row.key());
            LocalDate active=control;
            // A transaction ID is immutable even within a day's staging; identical repeats are safe.
            if (kind.equals("transaction")) {
                try (var s=db.prepareStatement("SELECT fixed_line FROM camel_ingest.record WHERE batch_date=? AND kind='transaction' AND business_key=?")) {
                    s.setObject(1,active); s.setString(2,row.key());
                    try (var r=s.executeQuery()) {
                        if (r.next() && !r.getString(1).equals(row.line())) throw new IllegalArgumentException("Conflicting transaction ID "+row.key());
                    }
                }
            }
            receipt(db,source,active);
            try (var s=db.prepareStatement("""
                INSERT INTO camel_ingest.record(batch_date,kind,business_key,fixed_line,topic,partition_id,source_offset)
                VALUES (?,?,?,?,?,?,?) ON CONFLICT (batch_date,kind,business_key)
                DO UPDATE SET fixed_line=excluded.fixed_line,topic=excluded.topic,
                  partition_id=excluded.partition_id,source_offset=excluded.source_offset
                WHERE camel_ingest.record.source_offset < excluded.source_offset
                """)) {
                s.setObject(1,active); s.setString(2,kind); s.setString(3,row.key()); s.setString(4,row.line());
                s.setString(5,source.topic()); s.setInt(6,source.partition()); s.setLong(7,source.offset()); s.executeUpdate();
            }
            return new Accepted(active,control,false,false);
        });
        // Report persisted state only after the transaction has committed.
        if (accepted.dateChanged()) {
            if (accepted.previousDate() == null) LOG.info("Batch date initialized batchDate={}",accepted.date());
            else {
                LOG.info("Batch date advanced previousDate={} batchDate={}",accepted.previousDate(),accepted.date());
                LOG.info("Batch closed batchDate={}",accepted.previousDate());
            }
        }
        LOG.debug("{} batchDate={} topic={} partition={} offset={}",
            accepted.duplicate() ? "Duplicate message skipped" : Topics.DATE.equals(source.topic()) ? "Batch date message persisted" : "Input message staged",
            accepted.date(),source.topic(),source.partition(),source.offset());
        return accepted.date();
    }
    private static void identity(PreparedStatement s,Source source) throws SQLException {
        s.setString(1,source.topic()); s.setInt(2,source.partition()); s.setLong(3,source.offset());
    }
    private static void receipt(Connection db,Source source,LocalDate date) throws SQLException {
        try (var s=db.prepareStatement("INSERT INTO camel_ingest.receipt(topic,partition_id,source_offset,batch_date,payload) VALUES (?,?,?,?,?)")) {
            identity(s,source); s.setObject(4,date); s.setString(5,source.payload()); s.executeUpdate();
        }
    }
    public static void execute(Connection db,String sql,Object value) throws SQLException {
        try (var s=db.prepareStatement(sql)) { s.setObject(1,value); s.executeUpdate(); }
    }
    public static void lock(Connection db,int id) throws SQLException {
        try (var s=db.prepareStatement("SELECT pg_advisory_xact_lock(64019,?)")) { s.setInt(1,id); s.execute(); }
    }
}
