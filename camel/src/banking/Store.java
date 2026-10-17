package banking;

import java.sql.*;
import java.time.LocalDate;

/** Each callback owns a fresh JDBC transaction. The clock row serializes assignment and closure. */
public final class Store {
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
    private final Settings settings;
    public Store(Settings settings) { this.settings=settings; }
    public <T> T inTransaction(Work<T> work) throws Exception {
        try (Connection db=settings.connect()) {
            db.setAutoCommit(false);
            try { T result=work.run(db); db.commit(); return result; }
            catch (Exception e) { try { db.rollback(); } catch (Exception rollback) { e.addSuppressed(rollback); } throw e; }
        }
    }
    public LocalDate accept(Source source) throws Exception {
        return inTransaction(db -> {
            LocalDate control;
            try (var s=db.createStatement(); var r=s.executeQuery("SELECT control_date FROM camel_ingest.clock WHERE singleton FOR UPDATE")) {
                if (!r.next()) throw new IllegalStateException("Missing clock singleton");
                control=r.getObject(1,LocalDate.class);
            }
            try (var s=db.prepareStatement("SELECT batch_date,payload FROM camel_ingest.receipt WHERE topic=? AND partition_id=? AND source_offset=?")) {
                identity(s,source);
                try (var r=s.executeQuery()) {
                    if (r.next()) {
                        if (!source.payload().equals(r.getString(2))) throw new IllegalArgumentException("Source identity reused with different payload");
                        return r.getObject(1,LocalDate.class);
                    }
                }
            }
            if (Topics.DATE.equals(source.topic())) {
                if (!"clock".equals(source.key())) throw new IllegalArgumentException("system.batch-date key must be clock");
                LocalDate next=Records.date(source.payload());
                LocalDate active=Records.date(next.plusDays(1).toString());
                if (control != null && !next.equals(control) && !next.equals(control.plusDays(1)))
                    throw new IllegalArgumentException("Date must repeat or advance exactly one calendar day; current="+control+", received="+next);
                if (control == null || !control.equals(next)) {
                    if (control != null) execute(db,"UPDATE camel_ingest.batch SET closed=TRUE WHERE batch_date=?",control.plusDays(1));
                    execute(db,"INSERT INTO camel_ingest.batch(batch_date) VALUES (?) ON CONFLICT DO NOTHING",active);
                    execute(db,"UPDATE camel_ingest.clock SET control_date=?,updated_at=now() WHERE singleton",next);
                }
                receipt(db,source,active);
                return active;
            }
            if (control == null) throw new AwaitingClock();
            String kind=source.topic().startsWith("input.") ? source.topic().substring(6) : "";
            Records.Row row=Records.decode(kind,source.payload());
            if (!row.key().equals(source.key())) throw new IllegalArgumentException("Kafka key must match business key "+row.key());
            LocalDate active=control.plusDays(1);
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
            return active;
        });
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
