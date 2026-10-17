package corebank.generator;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.time.format.DateTimeFormatter;
import java.util.*;
import corebank.generator.Generate.Request;
public final class FileGenerator {
    private static final String[] TYPES = {"customer","account","relationship","transaction"};
    public void generate(Connection db, Path output, Request request) throws Exception {
        if (!db.getAutoCommit()) throw new IllegalArgumentException("Generator requires a fresh auto-commit connection.");
        String suffix = "_" + request.date().format(DateTimeFormatter.BASIC_ISO_DATE) + ".dat";
        Files.createDirectories(output);
        for (String type : TYPES) if (Files.exists(output.resolve(type + suffix)))
            throw new IllegalArgumentException("Output already exists: " + output.resolve(type + suffix));
        if (Files.exists(output.resolve("batch_" + request.date().format(DateTimeFormatter.BASIC_ISO_DATE) + ".trg")))
            throw new IllegalArgumentException("A trigger already exists for this date.");
        var source = new Generate().prepare(db, request);
        Path temporary = Files.createTempDirectory(output, ".generate-");
        var published = new ArrayList<Path>();
        try {
            var writers = new LinkedHashMap<String, BufferedWriter>();
            try {
                for (String type : TYPES) writers.put(type, writer(temporary,type,suffix));
                source.writeTo(record -> line(writers.get(record.kind()), record.line()));
            } finally {
                Exception failure = null;
                for (var writer : writers.values()) try { writer.close(); }
                catch (Exception e) { if (failure == null) failure=e; else failure.addSuppressed(e); }
                if (failure != null) throw failure;
            }
            // No trigger is emitted. Consumers must start only after this command succeeds.
            for (String type : TYPES) {
                Path destination=output.resolve(type+suffix);
                Files.move(temporary.resolve(type+suffix), destination); // no replacement
                published.add(destination);
            }
        } catch (Exception failure) {
            for (Path path : published) try { Files.delete(path); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        } finally {
            try (var files=Files.list(temporary)) { for (Path path : files.toList()) Files.deleteIfExists(path); }
            Files.deleteIfExists(temporary);
        }
    }
    private static BufferedWriter writer(Path path,String type,String suffix) throws Exception {
        return Files.newBufferedWriter(path.resolve(type+suffix),StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);
    }
    private static void line(BufferedWriter writer,String value) throws Exception { writer.write(value); writer.write('\n'); }
}
