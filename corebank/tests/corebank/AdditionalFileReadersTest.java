package corebank;

import corebank.readers.RelationshipFileReader;
import corebank.readers.TransactionFileReader;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import static org.junit.jupiter.api.Assertions.*;

public class AdditionalFileReadersTest {
    @TempDir Path directory;
    private static String number(String s, int width) { return " ".repeat(width-s.length())+s; }
    private static String text(String s, int width) { return s+" ".repeat(width-s.length()); }
    private static String relationship(String account, String customer, String type) {
        return number(account,19)+number(customer,19)+text(type,9);
    }
    private static String transaction(String id, String account, String type, String amount) {
        return number(id,19)+number(account,19)+text(type,6)+number(amount,20)+"2026-10-09";
    }
    private static <T> T readOne(FlatFileItemReader<T> reader) throws Exception {
        try {
            reader.open(new ExecutionContext());
            T value=reader.read();
            assertNull(reader.read());
            return value;
        } finally { reader.close(); }
    }
    @ParameterizedTest @ValueSource(strings={"PRIMARY","SECONDARY"})
    void readsRelationship(String type) throws Exception {
        Path file=directory.resolve("relationship.dat");
        Files.writeString(file,relationship("9223372036854775807","1",type)+"\n");
        var value=readOne(RelationshipFileReader.create(file));
        assertEquals(Long.MAX_VALUE,value.accountId());
        assertEquals(1,value.customerId());
        assertEquals(type,value.type().name());
    }
    @ParameterizedTest @CsvSource({"CR,0","DR,0","CR,1000","DR,2500","CR,9223372036854775807","DR,9223372036854775807"})
    void readsTransaction(String type,String amount) throws Exception {
        Path file=directory.resolve("transaction.dat");
        Files.writeString(file,transaction("1","9223372036854775807",type,amount)+"\n");
        var value=readOne(TransactionFileReader.create(file));
        assertEquals(1,value.transactionId());
        assertEquals(Long.MAX_VALUE,value.accountId());
        assertEquals(type,value.type().name());
        assertEquals(Long.parseLong(amount),value.amount());
        assertEquals(java.time.LocalDate.of(2026,10,9),value.effectiveDate());
    }
    @ParameterizedTest @ValueSource(strings={"accountZero","accountNegative","customerZero","customerNegative","overflow","type","short","long","control"})
    void rejectsRelationship(String kind) throws Exception {
        String line=switch(kind) {
            case "accountZero" -> relationship("0","1","PRIMARY");
            case "accountNegative" -> relationship("-1","1","PRIMARY");
            case "customerZero" -> relationship("1","0","PRIMARY");
            case "customerNegative" -> relationship("1","-1","PRIMARY");
            case "overflow" -> relationship("1","9223372036854775808","PRIMARY");
            case "type" -> relationship("1","1","R1");
            default -> relationship("1","1","PRIMARY");
        };
        checkFailure("relationship",kind,line);
    }
    @ParameterizedTest @ValueSource(strings={"idZero","idNegative","accountZero","accountNegative","idOverflow","type","negativeAmount","plusAmount","decimal","emptyAmount","overflow","twentyDigits","missingDate","invalidDate","zeroYear","short","long","control"})
    void rejectsTransaction(String kind) throws Exception {
        String line=switch(kind) {
            case "idZero" -> transaction("0","1","CR","1");
            case "idNegative" -> transaction("-1","1","CR","1");
            case "accountZero" -> transaction("1","0","CR","1");
            case "accountNegative" -> transaction("1","-1","CR","1");
            case "idOverflow" -> transaction("9223372036854775808","1","CR","1");
            case "type" -> transaction("1","1","CREDIT","1");
            case "negativeAmount" -> transaction("1","1","DR","-1");
            case "plusAmount" -> transaction("1","1","CR","+1");
            case "decimal" -> transaction("1","1","CR","1.50");
            case "emptyAmount" -> transaction("1","1","CR","");
            case "overflow" -> transaction("1","1","CR","9223372036854775808");
            case "missingDate" -> transaction("1","1","CR","1").substring(0,64);
            case "invalidDate" -> transaction("1","1","CR","1").replace("2026-10-09","2026-02-30");
            case "zeroYear" -> transaction("1","1","CR","1").replace("2026-10-09","0000-10-09");
            case "twentyDigits" -> transaction("1","1","CR","99999999999999999999");
            default -> transaction("1","1","CR","1");
        };
        checkFailure("transaction",kind,line);
    }
    private void checkFailure(String name,String kind,String line) throws Exception {
        if(kind.equals("short"))line=line.substring(1);
        if(kind.equals("long"))line+=" ";
        if(kind.equals("control"))line=line.substring(0,line.length()-1)+"\t";
        Path file=directory.resolve(name+".dat");
        Files.writeString(file,line+"\n");
        FlatFileItemReader<?> reader=name.equals("relationship")?RelationshipFileReader.create(file):TransactionFileReader.create(file);
        try {
            reader.open(new ExecutionContext());
            var error=assertThrows(FlatFileParseException.class,reader::read);
            assertEquals(1,error.getLineNumber());
            assertTrue(error.getMessage().contains(name+".dat"));
        } finally { reader.close(); }
    }
}
