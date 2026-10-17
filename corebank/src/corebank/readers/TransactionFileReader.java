package corebank.readers;

import java.nio.file.Path;
import java.time.LocalDate;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import corebank.BankingRecords.Transaction;
import corebank.BankingRecords.TransactionType;

/** Reads fixed-width Transaction input; reference and duplicate checks belong to staging validation. */
public final class TransactionFileReader {
    private TransactionFileReader() {}

    public static FlatFileItemReader<Transaction> create(Path file) {
        var tokenizer = new CodePointLineTokenizer(
            new String[] {"transactionId", "accountId", "type", "amount", "effectiveDate"}, 19, 19, 6, 20, 10);
        var reader = new FlatFileItemReaderBuilder<Transaction>()
            .name("transactionFileReader").resource(new FileSystemResource(file))
            .strict(true).comments(new String[0]).lineTokenizer(tokenizer)
            .fieldSetMapper(f -> new Transaction(
                FileReaderSupport.positiveId(f.readString("transactionId")),
                FileReaderSupport.positiveId(f.readString("accountId")),
                TransactionType.valueOf(f.readString("type")),
                amount(f.readString("amount")), date(f.readRawString("effectiveDate"))))
            .build();
        FileReaderSupport.configure(reader);
        return reader;
    }

    private static LocalDate date(String value) {
        if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"))
            throw new IllegalArgumentException("Effective date must use YYYY-MM-DD");
        LocalDate date=LocalDate.parse(value);
        if (date.getYear()<1) throw new IllegalArgumentException("Effective date must have a positive year");
        return date;
    }

    private static long amount(String value) {
        if (!value.matches("[0-9]+"))
            throw new IllegalArgumentException("Amount must be nonnegative integer cents");
        return Long.parseLong(value);
    }
}
