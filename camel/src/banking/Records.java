package banking;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.JsonParser;
import java.text.Normalizer;
import java.time.LocalDate;
import java.util.*;

/** Strict JSON to the existing Unicode/code-point fixed-width contract. */
public final class Records {
    public static final ObjectMapper JSON = new ObjectMapper()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public record Row(String key, String line) {}
    public static LocalDate date(String text) {
        if (text == null || !text.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) throw new IllegalArgumentException("Expected YYYY-MM-DD");
        LocalDate date = LocalDate.parse(text);
        if (date.getYear() < 1 || date.getYear() > 9999) throw new IllegalArgumentException("Date outside four-digit year range");
        return date;
    }
    public static Row decode(String kind, String payload) throws Exception {
        if (payload == null) throw new IllegalArgumentException("Tombstones are not supported");
        JsonNode n = JSON.readTree(payload);
        if (n == null || !n.isObject()) throw new IllegalArgumentException("Expected JSON object");
        return switch (kind) {
            case "customer" -> {
                fields(n,"customerId","firstName","lastName","addressLine1","city","province","postalCode","country");
                long id = number(n,"customerId",true);
                yield new Row(""+id, num(id,19)+field(text(n,"firstName"),10)+field(text(n,"lastName"),10)
                    +field(text(n,"addressLine1"),20)+field(text(n,"city"),10)+field(text(n,"province"),2)
                    +field(text(n,"postalCode"),7)+field(text(n,"country"),10));
            }
            case "account" -> {
                fields(n,"accountId","startDate","endDate","accountType");
                long id = number(n,"accountId",true);
                LocalDate start = date(text(n,"startDate"));
                LocalDate end = n.get("endDate") == null || n.get("endDate").isNull() ? null : date(text(n,"endDate"));
                if (end != null && end.isBefore(start)) throw new IllegalArgumentException("endDate precedes startDate");
                yield new Row(""+id,num(id,19)+field(start.toString(),10)+field(end == null ? "" : end.toString(),10)
                    +field(choice(n,"accountType","SAVINGS","CHECKING"),8));
            }
            case "relationship" -> {
                fields(n,"accountId","customerId","type");
                long account = number(n,"accountId",true), customer = number(n,"customerId",true);
                yield new Row(account+":"+customer,num(account,19)+num(customer,19)+field(choice(n,"type","PRIMARY","SECONDARY"),9));
            }
            case "transaction" -> {
                fields(n,"transactionId","accountId","type","amount","effectiveDate");
                long id = number(n,"transactionId",true);
                yield new Row(""+id,num(id,19)+num(number(n,"accountId",true),19)+field(choice(n,"type","CR","DR"),6)
                    +num(number(n,"amount",false),20)+date(text(n,"effectiveDate")).toString());
            }
            default -> throw new IllegalArgumentException("Unknown kind: "+kind);
        };
    }
    private static void fields(JsonNode n, String... names) {
        var allowed = Set.of(names);
        n.fieldNames().forEachRemaining(f -> { if (!allowed.contains(f)) throw new IllegalArgumentException("Unknown field: "+f); });
    }
    private static String text(JsonNode n, String field) {
        if (!n.hasNonNull(field) || !n.get(field).isTextual()) throw new IllegalArgumentException("Expected string: "+field);
        return n.get(field).textValue();
    }
    private static String choice(JsonNode n,String field,String... choices) {
        String value = text(n,field);
        if (!Set.of(choices).contains(value)) throw new IllegalArgumentException("Invalid "+field);
        return value;
    }
    private static long number(JsonNode n,String field,boolean positive) {
        JsonNode value=n.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) throw new IllegalArgumentException("Expected BIGINT integer: "+field);
        long result=value.longValue();
        if (result < (positive ? 1 : 0)) throw new IllegalArgumentException("Out of range: "+field);
        return result;
    }
    public static String field(String value,int width) {
        String normalized=Normalizer.normalize(value,Normalizer.Form.NFC);
        if (normalized.codePoints().anyMatch(c -> Character.isISOControl(c) || c == 0xfeff || c == 0x2028 || c == 0x2029 || (c >= 0xd800 && c <= 0xdfff)))
            throw new IllegalArgumentException("Invalid character");
        int length=normalized.codePointCount(0,normalized.length());
        if (length > width) throw new IllegalArgumentException("Field exceeds width "+width);
        return normalized+" ".repeat(width-length);
    }
    private static String num(long n,int width) { String value=Long.toString(n); return " ".repeat(width-value.length())+value; }
    public static String parseBalanceLineToJson(String line,LocalDate date) throws Exception {
        if (!line.matches("[ 0-9]{19}[ 0-9-]{20}")) throw new IllegalArgumentException("Invalid balance record width/characters");
        String accountText=line.substring(0,19).strip(), amountText=line.substring(19).strip();
        if (!accountText.matches("[0-9]+") || !amountText.matches("-?[0-9]+")) throw new IllegalArgumentException("Invalid balance numbers");
        long account=Long.parseLong(accountText), amount=Long.parseLong(amountText);
        if (account <= 0) throw new IllegalArgumentException("Invalid account ID");
        return JSON.writeValueAsString(Map.of("batchDate",date.toString(),"accountId",account,"balance",amount));
    }
}
