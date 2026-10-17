package corebank.generator;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.LocalDate;
/** Compatibility API for existing file-generator callers. */
public final class BatchGenerator {
    public record Request(LocalDate date, int customers, int accounts, int relationships, int transactions, long seed) {
        public Request {
            if (date == null || date.getYear() < 1 || date.getYear() > 9999)
                throw new IllegalArgumentException("Date must have a four-digit positive year.");
            if (customers < 0 || accounts < 0 || relationships < 0 || transactions < 0)
                throw new IllegalArgumentException("Record counts must be nonnegative.");
        }
    }
    public void generate(Connection db, Path output, Request r) throws Exception {
        new FileGenerator().generate(db, output, new Generate.Request(r.date(),r.customers(),r.accounts(),r.relationships(),r.transactions(),r.seed()));
    }
    public static String field(String value,int width) { return Generate.field(value,width); }
}
