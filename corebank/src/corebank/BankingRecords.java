package corebank;

import java.time.LocalDate;
import java.util.Objects;
public final class BankingRecords {
    private BankingRecords() {
    }

    public record Customer(long customerId, String firstName, String lastName, String addressLine1, String city, String province, String postalCode, String country) {
        public Customer {
            Objects.requireNonNull(addressLine1, "addressLine1");
            Objects.requireNonNull(city, "city");
            Objects.requireNonNull(province, "province");
            Objects.requireNonNull(postalCode, "postalCode");
            Objects.requireNonNull(country, "country");
        }
    }
    public record Account(long accountId, LocalDate startDate, LocalDate endDate, AccountType accountType) {
    }
    public record Relationship(long accountId, long customerId, RelationshipType type) {
    }
    public record Transaction(long transactionId, long accountId, TransactionType type, long amount, LocalDate effectiveDate) {
    }

    public enum AccountType{
        SAVINGS, CHECKING
    }
    public enum TransactionType {
        CR, DR
    }
    public enum RelationshipType {
        PRIMARY, SECONDARY
    }
}
