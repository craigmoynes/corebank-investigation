package corebank.ingest;

import java.nio.file.Files;
import java.nio.file.Path;
import corebank.BatchLocks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;

import corebank.readers.AccountFileReader;
import corebank.readers.CustomerFileReader;
import corebank.readers.RelationshipFileReader;
import corebank.readers.TransactionFileReader;

/**
 * Atomically replaces a date's staging rows. Does not validate business keys or
 * post balances.
 */
public final class BatchIngestor {
    private static final int CHUNK_SIZE = 500;

    public Map<String, Long> ingest(Connection connection, Path input, LocalDate date) throws Exception {
        if (date == null || date.getYear() < 1 || date.getYear() > 9999)
            throw new IllegalArgumentException("Batch date must have a four-digit positive year");
        if (!connection.getAutoCommit())
            throw new IllegalArgumentException("Ingestion requires a connection with auto-commit enabled");
        String suffix = "_" + date.format(DateTimeFormatter.BASIC_ISO_DATE) + ".dat";
        for (String name : new String[] { "customer", "account", "relationship", "transaction" }) {
            Path file = input.resolve(name + suffix);
            if (!Files.isRegularFile(file))
                throw new IllegalArgumentException("Missing input file: " + file);
        }
        connection.setAutoCommit(false);
        try {
            // Serialize same-date replacement across application instances; lock ends with
            // transaction.
            try (var lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, ?)")) {
                lock.setInt(1, BatchLocks.DATE_LOCK_NAMESPACE);
                lock.setInt(2, BatchLocks.dateKey(date));
                lock.execute();
            }
            try (var status = connection.prepareStatement("SELECT status FROM core.batch_run WHERE batch_date=? AND status IN ('RUNNING','COMPLETED')")) {
                status.setObject(1, date);
                try (var rows = status.executeQuery()) {
                    if (rows.next()) throw new IllegalStateException("Cannot replace staging for " + rows.getString(1) + " batch " + date);
                }
            }
            Map<String, Long> counts = new LinkedHashMap<>();
            counts.put("customer", 
                load(connection, date, input.resolve("customer" + suffix), "customer",
                    "customer_id,first_name,last_name,address_line1,city,province,postal_code,country", 8,
                    CustomerFileReader.create(input.resolve("customer" + suffix)),
                        (s, r) -> {
                            s.setLong(4, r.customerId());
                            s.setString(5, r.firstName());
                            s.setString(6, r.lastName());
                            s.setString(7, r.addressLine1());
                            s.setString(8, r.city());
                            s.setString(9, r.province());
                            s.setString(10, r.postalCode());
                            s.setString(11, r.country());
                        }
                )
            );
            counts.put("account", load(connection, date, input.resolve("account" + suffix), "account",
                    "account_id,start_date,end_date,account_type", 4,
                    AccountFileReader.create(input.resolve("account" + suffix)),
                    (s, r) -> {
                        s.setLong(4, r.accountId());
                        s.setObject(5, r.startDate());
                        s.setObject(6, r.endDate());
                        s.setString(7, r.accountType().name());
                    }));
            counts.put("relationship", load(connection, date, input.resolve("relationship" + suffix), "relationship",
                    "account_id,customer_id,type", 3,
                    RelationshipFileReader.create(input.resolve("relationship" + suffix)),
                    (s, r) -> {
                        s.setLong(4, r.accountId());
                        s.setLong(5, r.customerId());
                        s.setString(6, r.type().name());
                    }));
            counts.put("transaction", load(connection, date, input.resolve("transaction" + suffix), "transaction",
                    "transaction_id,account_id,type,amount,effective_date", 5,
                    TransactionFileReader.create(input.resolve("transaction" + suffix)),
                    (s, r) -> {
                        s.setLong(4, r.transactionId());
                        s.setLong(5, r.accountId());
                        s.setString(6, r.type().name());
                        s.setLong(7, r.amount());
                        s.setObject(8, r.effectiveDate());
                    }));
            try (var receipt = connection.prepareStatement("INSERT INTO core_ingest.batch_receipt(batch_date) VALUES (?) ON CONFLICT(batch_date) DO UPDATE SET ingested_at=clock_timestamp()")) {
                receipt.setObject(1, date);
                receipt.executeUpdate();
            }
            connection.commit();
            return counts;
        } catch (Exception failure) {
            try {
                connection.rollback();
            } catch (Exception rollback) {
                failure.addSuppressed(rollback);
            }
            throw failure;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private <T> long load(Connection c, LocalDate date, Path file, String table, String columns,
            int fields, FlatFileItemReader<T> reader, Binder<T> binder) throws Exception {
        // Table/column names are internal constants, never user input.
        try (var delete = c.prepareStatement("DELETE FROM core_ingest." + table + " WHERE batch_date = ?")) {
            delete.setObject(1, date);
            delete.executeUpdate();
        }
        String placeholders = String.join(",", java.util.Collections.nCopies(fields + 3, "?"));
        try (var insert = c.prepareStatement("INSERT INTO core_ingest." + table
                + " (batch_date,source_file,source_line," + columns + ") VALUES (" + placeholders + ")")) {
            reader.open(new ExecutionContext());
            long line = 0;
            T row;
            while ((row = reader.read()) != null) {
                insert.setObject(1, date);
                insert.setString(2, file.getFileName().toString());
                insert.setLong(3, ++line);
                binder.bind(insert, row);
                insert.addBatch();
                if (line % CHUNK_SIZE == 0) {
                    insert.executeBatch();
                    insert.clearBatch();
                }
            }
            if (line % CHUNK_SIZE != 0)
                insert.executeBatch();
            return line;
        } finally {
            reader.close();
        }
    }

    @FunctionalInterface
    private interface Binder<T> {
        void bind(PreparedStatement statement, T record) throws Exception;
    }
}
