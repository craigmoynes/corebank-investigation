package banking;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

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
        if (!Topics.DATE.equals(source.topic())) return acceptBatch(List.of(source)).getFirst();
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
            throw new IllegalArgumentException("Unsupported control topic");
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
    /** One bounded data batch, one clock lock and one commit. Dates retain their control path. */
    public List<LocalDate> acceptBatch(List<Source> sources) throws Exception {
        if (sources.isEmpty() || sources.size()>100) throw new IllegalArgumentException("Batch size must be 1..100");
        String topic=sources.getFirst().topic();
        if (!topic.startsWith("input.") || sources.stream().anyMatch(s -> !s.topic().equals(topic)))
            throw new IllegalArgumentException("A data batch must contain one input topic");
        return inTransaction(db -> {
            LocalDate active;
            try (var s=db.createStatement(); var r=s.executeQuery("SELECT active_date FROM camel_ingest.clock WHERE singleton FOR UPDATE")) {
                if (!r.next()) throw new IllegalStateException("Missing clock singleton");
                active=r.getObject(1,LocalDate.class);
            }
            record Receipt(LocalDate date,String payload) {}
            var receipts=new HashMap<Long,Receipt>();
            try (var s=db.prepareStatement("SELECT source_offset,batch_date,payload FROM camel_ingest.receipt WHERE topic=? AND partition_id=0 AND source_offset IN ("+placeholders(sources.size())+")")) {
                s.setString(1,topic);
                for(int i=0;i<sources.size();i++) s.setLong(i+2,sources.get(i).offset());
                try(var r=s.executeQuery()) { while(r.next()) receipts.put(r.getLong(1),new Receipt(r.getObject(2,LocalDate.class),r.getString(3))); }
            }
            String kind=topic.substring(6);
            var dates=new ArrayList<LocalDate>();
            var fresh=new ArrayList<Source>();
            record Staged(Source source,Records.Row row) {}
            var staged=new LinkedHashMap<String,Staged>();
            for(var source:sources) {
                var prior=receipts.get(source.offset());
                if(prior!=null) {
                    if(!prior.payload().equals(source.payload())) throw new IllegalArgumentException("Source identity reused with different payload");
                    dates.add(prior.date()); continue;
                }
                if(active==null) throw new AwaitingClock();
                var row=Records.decode(kind,source.payload());
                if(!row.key().equals(source.key())) throw new IllegalArgumentException("Kafka key must match business key "+row.key());
                var previous=staged.get(row.key());
                if(kind.equals("transaction") && previous!=null && !previous.row().line().equals(row.line()))
                    throw new IllegalArgumentException("Conflicting transaction ID "+row.key());
                if(previous==null || previous.source().offset()<source.offset()) staged.put(row.key(),new Staged(source,row));
                fresh.add(source); receipts.put(source.offset(),new Receipt(active,source.payload())); dates.add(active);
            }
            if(fresh.isEmpty()) return List.copyOf(dates);
            if(kind.equals("transaction")) {
                try(var s=db.prepareStatement("SELECT business_key,fixed_line FROM camel_ingest.record WHERE batch_date=? AND kind='transaction' AND business_key IN ("+placeholders(staged.size())+")")) {
                    s.setObject(1,active); int i=2;
                    for(var key:staged.keySet()) s.setString(i++,key);
                    try(var r=s.executeQuery()) { while(r.next()) {
                        if(!staged.get(r.getString(1)).row().line().equals(r.getString(2)))
                            throw new IllegalArgumentException("Conflicting transaction ID "+r.getString(1));
                    } }
                }
            }
            try(var s=db.prepareStatement("INSERT INTO camel_ingest.receipt(topic,partition_id,source_offset,batch_date,payload) VALUES "+values(fresh.size(),5))) {
                int i=1;
                for(var source:fresh) {
                    s.setString(i++,topic); s.setInt(i++,source.partition()); s.setLong(i++,source.offset());
                    s.setObject(i++,active); s.setString(i++,source.payload());
                }
                s.executeUpdate();
            }
            try(var s=db.prepareStatement("INSERT INTO camel_ingest.record(batch_date,kind,business_key,fixed_line,topic,partition_id,source_offset) VALUES "+values(staged.size(),7)+" ON CONFLICT (batch_date,kind,business_key) DO UPDATE SET fixed_line=excluded.fixed_line,topic=excluded.topic,partition_id=excluded.partition_id,source_offset=excluded.source_offset WHERE camel_ingest.record.source_offset < excluded.source_offset")) {
                int i=1;
                for(var entry:staged.values()) {
                    s.setObject(i++,active); s.setString(i++,kind); s.setString(i++,entry.row().key()); s.setString(i++,entry.row().line());
                    s.setString(i++,topic); s.setInt(i++,entry.source().partition()); s.setLong(i++,entry.source().offset());
                }
                s.executeUpdate();
            }
            return List.copyOf(dates);
        });
    }
    private static String placeholders(int count) { return String.join(",",Collections.nCopies(count,"?")); }
    private static String values(int rows,int columns) { return String.join(",",Collections.nCopies(rows,"("+placeholders(columns)+")")); }
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
