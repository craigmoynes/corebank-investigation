package banking;

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/** Database/file/broker boundaries are at-least-once, with explicit recovery receipts. */
public final class FilesBridge {
    @FunctionalInterface public interface Publish { void send(String key,String json) throws Exception; }
    private final Store store;
    private final Settings settings;
    public FilesBridge(Store store,Settings settings) { this.store=store; this.settings=settings; }
    /**
     * Extracts the oldest closed batch that has no extraction receipt, if available.
     * @return true when extraction completes (including recovery); false when no batch is eligible
     * @throws Exception if extraction fails; failures are not treated as absence of work
     */
    public boolean extractNextClosedBatchIfAvailable() throws Exception {
        return store.inTransaction(db -> {
            Store.lock(db,1);
            LocalDate date;
            try (var s=db.createStatement(); var r=s.executeQuery("SELECT batch_date FROM camel_ingest.batch WHERE closed AND NOT extracted ORDER BY batch_date LIMIT 1 FOR UPDATE")) {
                if (!r.next()) return false;
                date=r.getObject(1,LocalDate.class);
            }
            Files.createDirectories(settings.input());
            String stamp=date.format(DateTimeFormatter.BASIC_ISO_DATE);
            // Never recreate a consumed trigger after a crash between publication and DB commit.
            boolean consumed=Files.exists(settings.input().resolve("batch_"+stamp+".INPROGRESS.trg"))
                || Files.exists(settings.output().resolve("batch_"+stamp+".COMPLETE.trg"))
                || Files.exists(settings.error().resolve("batch_"+stamp+".ERROR.trg"));
            for (String kind : Topics.KINDS) {
                Path temp=Files.createTempFile(settings.input(),".camel-", ".tmp");
                try {
                    try (var writer=Files.newBufferedWriter(temp,StandardCharsets.UTF_8);
                         var s=db.prepareStatement("SELECT fixed_line FROM camel_ingest.record WHERE batch_date=? AND kind=? ORDER BY business_key")) {
                        s.setObject(1,date); s.setString(2,kind); s.setFetchSize(500);
                        try (var r=s.executeQuery()) { while(r.next()) { writer.write(r.getString(1)); writer.write('\n'); } }
                    }
                    Path target=settings.input().resolve(kind+"_"+stamp+".dat");
                    if (consumed && !Files.exists(target)) throw new IllegalStateException("Consumed batch is missing input "+target);
                    publish(temp,target);
                } finally { Files.deleteIfExists(temp); }
            }
            if (!consumed) {
                Path temp=Files.createTempFile(settings.input(),".camel-trigger-", ".tmp");
                try { publish(temp,settings.input().resolve("batch_"+stamp+".trg")); }
                finally { Files.deleteIfExists(temp); }
            }
            Store.execute(db,"UPDATE camel_ingest.batch SET extracted=TRUE WHERE batch_date=?",date);
            return true;
        });
    }
    private static void publish(Path temporary,Path target) throws Exception {
        if (Files.exists(target,LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isRegularFile(target,LinkOption.NOFOLLOW_LINKS) || Files.mismatch(temporary,target) != -1)
                throw new IllegalStateException("Conflicting existing file: "+target);
            return;
        }
        // Cooperating extractors hold the DB lock. No non-atomic publication fallback.
        Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);
    }
    public int publishPendingBalanceFiles(Publish sender) throws Exception {
        if (!Files.exists(settings.output())) return 0;
        try (var files=Files.list(settings.output())) {
            int count=0;
            for (Path path : files.filter(p -> p.getFileName().toString().matches("balance_[0-9]{8}\\.dat")).sorted().toList())
                if (publishBalanceFileIfUnpublished(path,sender)) count++;
            return count;
        }
    }
    private boolean publishBalanceFileIfUnpublished(Path path,Publish sender) throws Exception {
        return store.inTransaction(db -> {
            Store.lock(db,2);
            if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IllegalArgumentException("Not a regular balance file: "+path);
            String filename=path.getFileName().toString();
            LocalDate date=LocalDate.parse(filename.substring(8,16),DateTimeFormatter.BASIC_ISO_DATE);
            String content=Files.readString(path,StandardCharsets.UTF_8); // strict UTF-8; validate whole file before sending
            String hash=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)));
            try (var s=db.prepareStatement("SELECT sha256 FROM camel_ingest.publication WHERE filename=?")) {
                s.setString(1,filename);
                try (var r=s.executeQuery()) {
                    if (r.next()) {
                        if (!hash.equals(r.getString(1))) throw new IllegalArgumentException("Published balance file changed: "+filename);
                        return false;
                    }
                }
            }
            var messages=new ArrayList<String>();
            var keys=new ArrayList<String>();
            long previous=0;
            if (!content.isEmpty()) {
                if (!content.endsWith("\n")) throw new IllegalArgumentException("Balance file must end with LF");
                for (String line : content.substring(0,content.length()-1).split("\n",-1)) {
                    String json=Records.parseBalanceLineToJson(line,date);
                    long account=Records.JSON.readTree(json).get("accountId").longValue();
                    if (account <= previous) throw new IllegalArgumentException("Balance accounts must be unique and ascending");
                    previous=account;
                    messages.add(json); keys.add(date+":"+account);
                }
            }
            for (int i=0;i<messages.size();i++) sender.send(keys.get(i),messages.get(i));
            try (var s=db.prepareStatement("INSERT INTO camel_ingest.publication(filename,sha256) VALUES (?,?)")) {
                s.setString(1,filename); s.setString(2,hash); s.executeUpdate();
            }
            return true;
        });
    }
}
