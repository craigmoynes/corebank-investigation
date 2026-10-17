package corebank.processing;

import corebank.BatchLocks;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/** Posts one ingested date atomically. Owns the supplied dedicated JDBC connection's transactions. */
public final class BatchProcessor {
    public Map<String, Long> process(Connection c, LocalDate date) throws SQLException {
        if (date == null || date.getYear() < 1 || date.getYear() > 9999)
            throw new IllegalArgumentException("Batch date must have a four-digit positive year");
        if (!c.getAutoCommit()) throw new IllegalArgumentException("Processing requires auto-commit enabled");
        boolean globalLocked = false, dateLocked = false;
        Long run = null;
        try {
            // Session locks survive the attempt's initial commit. All processors serialize;
            // ingestion shares the date lock, so its four tables cannot change underneath us.
            lock(c, BatchLocks.PROCESS_LOCK_NAMESPACE, 0, true); globalLocked = true;
            lock(c, BatchLocks.DATE_LOCK_NAMESPACE, BatchLocks.dateKey(date), true); dateLocked = true;
            try (var s = c.prepareStatement("SELECT status,customer_count,account_count,relationship_count,transaction_count FROM core.batch_run WHERE batch_date=? AND status IN ('RUNNING','COMPLETED')")) {
                s.setObject(1, date);
                try (var rs = s.executeQuery()) {
                    if (rs.next()) {
                        if (rs.getString(1).equals("RUNNING"))
                            throw new SQLException("Batch has an unfinished RUNNING attempt; investigate before retrying: " + date);
                        var counts = new LinkedHashMap<String, Long>();
                        int column = 2;
                        for (String name : new String[]{"customer","account","relationship","transaction"})
                            counts.put(name, rs.getLong(column++));
                        return counts;
                    }
                }
            }
            reject(c, "SELECT 'An unfinished RUNNING attempt requires investigation: ' || batch_date FROM core.batch_run WHERE status='RUNNING' AND batch_date<>? LIMIT 1", date);
            reject(c, "SELECT 'Date has not been successfully ingested' WHERE NOT EXISTS (SELECT 1 FROM core_ingest.batch_receipt WHERE batch_date=?)", date);
            reject(c, "SELECT 'Cannot process a date older than a completed batch' FROM core.batch_run WHERE status='COMPLETED' AND batch_date>? LIMIT 1", date);
            try (var s = c.prepareStatement("INSERT INTO core.batch_run(batch_date) VALUES (?) RETURNING batch_run_id")) {
                s.setObject(1, date);
                try (var rs = s.executeQuery()) { rs.next(); run = rs.getLong(1); }
            }
            c.setAutoCommit(false);
            validate(c, date);
            // Capture genuinely new IDs before reference upserts. Reference-only updates
            // must not qualify existing accounts for output. The temporary table rolls
            // back/drops with this transaction and never changes operational state.
            try (var statement = c.prepareStatement("CREATE TEMP TABLE corebank_new_accounts ON COMMIT DROP AS SELECT account_id FROM core_ingest.account a WHERE batch_date=? AND NOT EXISTS (SELECT 1 FROM core.account old WHERE old.account_id=a.account_id)")) {
                statement.setObject(1, date); statement.execute();
            }
            var counts = new LinkedHashMap<String, Long>();
            counts.put("customer", upsert(c, date, "customer", "customer_id", "first_name,last_name,address_line1,city,province,postal_code,country"));
            counts.put("account", upsert(c, date, "account", "account_id", "start_date,end_date,account_type"));
            counts.put("relationship", upsert(c, date, "relationship", "account_id,customer_id", "type"));
            // Validate against the effective account state after this batch's upserts.
            reject(c, "SELECT 'Ineligible transaction at ' || t.source_file || ':' || t.source_line FROM core_ingest.transaction t JOIN core.account a USING(account_id) LEFT JOIN core.transaction old USING(transaction_id) WHERE t.batch_date=? AND old.transaction_id IS NULL AND (a.end_date IS NOT NULL OR a.start_date>t.effective_date) LIMIT 1", date);
            long posted;
            try (var s = c.prepareStatement("INSERT INTO core.transaction(transaction_id,account_id,type,amount,effective_date,batch_date,batch_run_id) SELECT t.transaction_id,t.account_id,t.type,t.amount,t.effective_date,t.batch_date,? FROM core_ingest.transaction t WHERE t.batch_date=? AND NOT EXISTS (SELECT 1 FROM core.transaction old WHERE old.transaction_id=t.transaction_id)")) {
                s.setLong(1, run); s.setObject(2, date); posted = s.executeLargeUpdate();
            }
            // NUMERIC aggregation avoids intermediate BIGINT overflow. The final cast rejects
            // balances outside signed cents range and rolls back the entire batch.
            try (var s = c.prepareStatement("UPDATE core.balance b SET balance=(b.balance::numeric+d.delta)::bigint FROM (SELECT account_id,SUM(CASE WHEN type='CR' THEN amount::numeric ELSE -amount::numeric END) delta FROM core.transaction WHERE batch_run_id=? GROUP BY account_id) d WHERE b.account_id=d.account_id")) {
                s.setLong(1, run); s.executeUpdate();
            }
            reject(c, "SELECT 'Missing balance for transaction account' FROM core_ingest.transaction t LEFT JOIN core.balance b USING(account_id) WHERE t.batch_date=? AND b.account_id IS NULL LIMIT 1", date);
            snapshot(c, date, run);
            counts.put("transaction", posted);
            try (var s = c.prepareStatement("UPDATE core.batch_run SET status='COMPLETED',completed_at=clock_timestamp(),customer_count=?,account_count=?,relationship_count=?,transaction_count=? WHERE batch_run_id=?")) {
                int i = 1; for (long count : counts.values()) s.setLong(i++, count);
                s.setLong(5, run); s.executeUpdate();
            }
            c.commit();
            return counts;
        } catch (SQLException | RuntimeException failure) {
            if (run != null) {
                try {
                    c.rollback(); c.setAutoCommit(true);
                    // If commit succeeded but its acknowledgement was lost, do not turn a
                    // completed run into FAILED. A retry will read its committed status.
                    try (var s = c.prepareStatement("UPDATE core.batch_run SET status='FAILED',completed_at=clock_timestamp(),error_message=? WHERE batch_run_id=? AND status='RUNNING'")) {
                        s.setString(1, failure.getMessage()); s.setLong(2, run); s.executeUpdate();
                    }
                } catch (SQLException cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        } finally {
            // Callers use a dedicated connection and close it even if cleanup fails.
            try { if (!c.getAutoCommit()) { c.rollback(); c.setAutoCommit(true); } }
            finally {
                try { if (dateLocked) lock(c, BatchLocks.DATE_LOCK_NAMESPACE, BatchLocks.dateKey(date), false); }
                finally { if (globalLocked) lock(c, BatchLocks.PROCESS_LOCK_NAMESPACE, 0, false); }
            }
        }
    }

    private static void snapshot(Connection c, LocalDate date, long run) throws SQLException {
        String selected = "SELECT account_id FROM corebank_new_accounts UNION SELECT account_id FROM core.transaction WHERE batch_run_id=?";
        long expected;
        try (var s = c.prepareStatement("INSERT INTO core_output.batch_snapshot(batch_date,batch_run_id,record_count) SELECT ?,?,count(*) FROM (" + selected + ") accounts RETURNING record_count")) {
            s.setObject(1, date); s.setLong(2, run); s.setLong(3, run);
            try (var rs = s.executeQuery()) { rs.next(); expected = rs.getLong(1); }
        }
        try (var s = c.prepareStatement("INSERT INTO core_output.balance(batch_date,account_id,balance) SELECT ?,b.account_id,b.balance FROM core.balance b JOIN (" + selected + ") accounts USING(account_id)")) {
            s.setObject(1, date); s.setLong(2, run);
            if (s.executeLargeUpdate() != expected)
                throw new SQLException("Missing balance for output account");
        }
    }

    private static void lock(Connection c, int namespace, int key, boolean acquire) throws SQLException {
        try (var s = c.prepareStatement("SELECT pg_advisory_" + (acquire ? "lock" : "unlock") + "(?,?)")) {
            s.setInt(1, namespace); s.setInt(2, key); s.execute();
        }
    }

    private static void reject(Connection c, String sql, LocalDate date) throws SQLException {
        try (var s = c.prepareStatement(sql)) {
            s.setObject(1, date);
            try (var rs = s.executeQuery()) { if (rs.next()) throw new SQLException(rs.getString(1)); }
        }
    }

    private static void validate(Connection c, LocalDate date) throws SQLException {
        String[][] keys = {{"customer","customer_id"},{"account","account_id"},{"relationship","account_id,customer_id"},{"transaction","transaction_id"}};
        for (String[] key : keys)
            reject(c, "SELECT 'Duplicate " + key[0] + " business key; first source line ' || min(source_line) FROM core_ingest." + key[0] + " WHERE batch_date=? GROUP BY " + key[1] + " HAVING count(*)>1 LIMIT 1", date);
        reject(c, "SELECT 'Account end date precedes start date at ' || source_file || ':' || source_line FROM core_ingest.account WHERE batch_date=? AND end_date<start_date LIMIT 1", date);
        reject(c, "SELECT 'Conflicting transaction ID at ' || t.source_file || ':' || t.source_line FROM core_ingest.transaction t JOIN core.transaction old USING(transaction_id) WHERE t.batch_date=? AND (t.account_id,t.type,t.amount,t.effective_date) IS DISTINCT FROM (old.account_id,old.type,old.amount,old.effective_date) LIMIT 1", date);
        // Missing references are checked against both existing and same-batch records.
        for (String table : new String[]{"relationship","transaction"})
            reject(c, "SELECT 'Missing account at ' || t.source_file || ':' || t.source_line FROM core_ingest." + table + " t WHERE t.batch_date=? AND NOT EXISTS (SELECT 1 FROM core.account a WHERE a.account_id=t.account_id) AND NOT EXISTS (SELECT 1 FROM core_ingest.account a WHERE a.batch_date=t.batch_date AND a.account_id=t.account_id) LIMIT 1", date);
        reject(c, "SELECT 'Missing customer at ' || t.source_file || ':' || t.source_line FROM core_ingest.relationship t WHERE t.batch_date=? AND NOT EXISTS (SELECT 1 FROM core.customer a WHERE a.customer_id=t.customer_id) AND NOT EXISTS (SELECT 1 FROM core_ingest.customer a WHERE a.batch_date=t.batch_date AND a.customer_id=t.customer_id) LIMIT 1", date);
    }

    private static long upsert(Connection c, LocalDate date, String table, String key, String fields) throws SQLException {
        String columns = key + "," + fields;
        String assignments = java.util.Arrays.stream(fields.split(",")).map(f -> f + "=EXCLUDED." + f).collect(java.util.stream.Collectors.joining(","));
        try (var s = c.prepareStatement("INSERT INTO core." + table + " (" + columns + ") SELECT " + columns + " FROM core_ingest." + table + " WHERE batch_date=? ON CONFLICT (" + key + ") DO UPDATE SET " + assignments)) {
            s.setObject(1, date); return s.executeLargeUpdate();
        }
    }
}
