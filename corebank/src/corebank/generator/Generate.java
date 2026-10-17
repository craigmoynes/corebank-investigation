package corebank.generator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.Normalizer;
import java.time.LocalDate;
import java.util.*;

/** Development generator; reads core/staging, never reserves IDs or writes database rows. */
public final class Generate {
    public record Request(LocalDate date, int customers, int accounts, int relationships, int transactions, long seed) {
        public Request {
            if (date == null || date.getYear() < 1 || date.getYear() > 9999)
                throw new IllegalArgumentException("Date must have a four-digit positive year.");
            if (customers < 0 || accounts < 0 || relationships < 0 || transactions < 0)
                throw new IllegalArgumentException("Record counts must be nonnegative.");
        }
    }
    private record Pair(long account, long customer) {}
    private record Snapshot(List<Long> customers, List<Long> accounts, List<Long> eligible,
                            Set<Pair> pairs, Set<Long> customerIds, Set<Long> accountIds, Set<Long> transactionIds) {}

    @FunctionalInterface public interface Sink { void accept(GeneratedRecord record) throws Exception; }
    @FunctionalInterface public interface Source { void writeTo(Sink sink) throws Exception; }
    public Source prepare(Connection db, Request request) throws Exception {
        if (!db.getAutoCommit()) throw new IllegalArgumentException("Generator requires a fresh auto-commit connection.");
        var snapshot = snapshot(db, request.date());
        var random = new Random(request.seed() ^ request.date().toEpochDay());
        // Validate feasibility before allocating files or generating records.
        long accountCount = (long)snapshot.accounts.size() + request.accounts();
        long customerCount = (long)snapshot.customers.size() + request.customers();
        long availablePairs = Math.multiplyExact(accountCount, customerCount) - snapshot.pairs.size();
        if (request.relationships() > availablePairs)
            throw new IllegalArgumentException("Requested " + request.relationships() + " relationships, but only " + availablePairs + " unused account/customer pairs exist. Generate more customers/accounts.");
        if (request.transactions() > 0 && snapshot.eligible.isEmpty() && request.accounts() == 0)
            throw new IllegalArgumentException("No eligible accounts: transactions require an account with no end date and start_date <= batch date. Include --accounts or populate core first.");
        List<String> first = request.customers() > 0 ? values("firstnames", 10) : List.of();
        List<String> last = request.customers() > 0 ? values("lastnames", 10) : List.of();
        // Reserve five positions for a four-digit house number and its separating space.
        List<String> streets = request.customers() > 0 ? values("streets", 15) : List.of();
        var used = new java.util.concurrent.atomic.AtomicBoolean();
        return sink -> {
            if (!used.compareAndSet(false,true)) throw new IllegalStateException("Prepared generation is single-use");
            {
                for (int i=0; i<request.customers(); i++) {
                    long id = id(random, snapshot.customerIds); snapshot.customers.add(id);
                    sink.accept(new GeneratedRecord("customer", Long.toString(id),
                        List.of("customerId","firstName","lastName","addressLine1","city","province","postalCode","country"),
                        List.of(id, first.get(random.nextInt(first.size())), last.get(random.nextInt(last.size())),
                            (1+random.nextInt(9999))+" "+streets.get(random.nextInt(streets.size())), "Toronto", "ON", "M5V 2T6", "Canada"),
                        List.of(19,10,10,20,10,2,7,10)));
                }
            }
            {
                for (int i=0; i<request.accounts(); i++) {
                    long id = id(random, snapshot.accountIds); snapshot.accounts.add(id); snapshot.eligible.add(id);
                    sink.accept(new GeneratedRecord("account", Long.toString(id),
                        List.of("accountId","startDate","endDate","accountType"),
                        Arrays.asList(id, request.date().toString(), null, random.nextBoolean()?"SAVINGS":"CHECKING"), List.of(19,10,10,8)));
                }
            }
            {
                // Shuffle pools, then scan without materializing the Cartesian product or retry loops.
                Collections.shuffle(snapshot.accounts, random); Collections.shuffle(snapshot.customers, random);
                int remaining=request.relationships();
                outer: for (long account : snapshot.accounts) for (long customer : snapshot.customers) {
                    if (remaining == 0) break outer;
                    if (snapshot.pairs.contains(new Pair(account,customer))) continue;
                    sink.accept(new GeneratedRecord("relationship", account+":"+customer, List.of("accountId","customerId","type"),
                        List.of(account,customer,random.nextBoolean()?"PRIMARY":"SECONDARY"), List.of(19,19,9)));
                    remaining--;
                }
            }
            {
                for (int i=0; i<request.transactions(); i++) {
                    long account=snapshot.eligible.get(random.nextInt(snapshot.eligible.size()));
                    long transaction=id(random,snapshot.transactionIds);
                    sink.accept(new GeneratedRecord("transaction", Long.toString(transaction), List.of("transactionId","accountId","type","amount"),
                        List.of(transaction,account,random.nextBoolean()?"CR":"DR",1L+random.nextInt(100000)), List.of(19,19,6,20)));
                }
            }
        };
    }
    private Snapshot snapshot(Connection db, LocalDate date) throws SQLException {
        boolean oldReadOnly=db.isReadOnly(); int oldIsolation=db.getTransactionIsolation();
        db.setReadOnly(true); db.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); db.setAutoCommit(false);
        try {
            var customers=ids(db,"SELECT customer_id FROM core.customer ORDER BY customer_id");
            var accounts=ids(db,"SELECT account_id FROM core.account ORDER BY account_id");
            var eligible=new ArrayList<Long>();
            try (var s=db.prepareStatement("SELECT account_id FROM core.account WHERE end_date IS NULL AND start_date <= ? ORDER BY account_id")) {
                s.setObject(1,date); try(var rows=s.executeQuery()) { while(rows.next()) eligible.add(rows.getLong(1)); }
            }
            var pairs=new HashSet<Pair>();
            try(var s=db.createStatement(); var rows=s.executeQuery("SELECT account_id,customer_id FROM core.relationship")) {
                while(rows.next()) pairs.add(new Pair(rows.getLong(1),rows.getLong(2)));
            }
            var result=new Snapshot(customers,accounts,eligible,pairs,
                used(db,"customer","customer_id"), used(db,"account","account_id"), used(db,"transaction","transaction_id"));
            db.commit(); return result;
        } catch (SQLException ex) { db.rollback(); throw ex; }
        finally { db.setAutoCommit(true); db.setTransactionIsolation(oldIsolation); db.setReadOnly(oldReadOnly); }
    }
    private static ArrayList<Long> ids(Connection db,String query) throws SQLException {
        var result=new ArrayList<Long>();
        try(var s=db.createStatement(); var rows=s.executeQuery(query)) { while(rows.next()) result.add(rows.getLong(1)); }
        return result;
    }
    private static Set<Long> used(Connection db,String table,String key) throws SQLException {
        return new HashSet<>(ids(db,"SELECT "+key+" FROM core."+table+" UNION SELECT "+key+" FROM core_ingest."+table));
    }
    private static List<String> values(String name, int width) throws IOException {
        String resource = "/generator/" + name + ".txt";
        var stream = Generate.class.getResourceAsStream(resource);
        if (stream == null) throw new IOException("Missing generator resource: " + resource);
        var result = new ArrayList<String>();
        try (var reader = new BufferedReader(new InputStreamReader(stream,
                StandardCharsets.UTF_8.newDecoder()))) {
            String line; int number = 0;
            while ((line = reader.readLine()) != null) {
                number++;
                if (line.isBlank() || line.startsWith("#")) continue;
                try {
                    field(line, width); // Enforce the same Unicode and width contract as output.
                    result.add(Normalizer.normalize(line, Normalizer.Form.NFC));
                } catch (IllegalArgumentException failure) {
                    throw new IOException(resource + ":" + number + ": " + failure.getMessage(), failure);
                }
            }
        }
        if (result.isEmpty()) throw new IOException("No values in generator resource: " + resource);
        return List.copyOf(result);
    }
    private static long id(Random random, Set<Long> used) {
        long id; do { id=random.nextLong() & Long.MAX_VALUE; } while(id==0 || !used.add(id)); return id;
    }
    public static String field(String value,int width) {
        String text=Normalizer.normalize(Objects.requireNonNull(value),Normalizer.Form.NFC);
        if(text.codePoints().anyMatch(c -> Character.isISOControl(c)||c==0xFEFF||c==0x2028||c==0x2029||(c>=0xD800&&c<=0xDFFF)))
            throw new IllegalArgumentException("Invalid character in field.");
        int length=text.codePointCount(0,text.length());
        if(length>width) throw new IllegalArgumentException("Field exceeds width "+width);
        return text+" ".repeat(width-length);
    }
}
