///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.kafka:kafka-clients:4.1.1
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//SOURCES banking/KafkaGenerator.java banking/Records.java banking/Topics.java
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.postgresql:postgresql
//DEPS org.springframework.boot:spring-boot-starter
//DEPS info.picocli:picocli:4.7.7
//SOURCES ../../corebank/src/corebank/generator/Generate.java ../../corebank/src/corebank/generator/GeneratedRecord.java ../../corebank/src/corebank/generator/FileGenerator.java ../../corebank/src/corebank/generator/BatchGenerator.java ../../corebank/src/corebank/generator/GeneratorConfiguration.java ../../corebank/src/corebank/Config.java
//FILES application.yaml=../../corebank/application.yaml
//FILES generator/firstnames.txt=../../corebank/src/resources/firstnames.txt
//FILES generator/lastnames.txt=../../corebank/src/resources/lastnames.txt
//FILES generator/streets.txt=../../corebank/src/resources/streets.txt

import corebank.generator.Generate;
import banking.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import java.util.Map;
import corebank.generator.GeneratorConfiguration;
import corebank.Config;
import picocli.CommandLine;
import picocli.CommandLine.*;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.util.concurrent.Callable;

@Command(name="kafka-generator", mixinStandardHelpOptions=true,
    description="Generate JSON Kafka records using a read-only database snapshot.")
public class KafkaGenerator implements Callable<Integer> {
    @Option(names="--date", description="Business date (YYYY-MM-DD); required when selecting data record types.") LocalDate date;
    @Option(names="--all", description="Default all four counts to 10; explicit counts override.") boolean all;
    @Option(names="--customers", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer customers;
    @Option(names="--accounts", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer accounts;
    @Option(names="--relationships", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer relationships;
    @Option(names="--transactions", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer transactions;
    @Option(names="--brokers", description="Kafka bootstrap servers; defaults to CAMEL_BROKERS or localhost:19092.") String brokers;
    @Option(names="--batch-date", description="Active business date to publish; may be used alone. With data, must equal --date.") LocalDate batchDate;
    @Option(names="--max-pending", defaultValue="1000") int maxPending;
    @Option(names="--topic", description="Override kind=topic (customer, account, relationship, transaction, batch-date).") Map<String,String> overrides;
    @Option(names="--seed", defaultValue="42", description="Random seed (default: ${DEFAULT-VALUE}).") long seed;
    @Option(names="--db-url", description="Override corebank.database.url from Spring configuration.") String url;
    @Option(names="--db-user", description="Override corebank.database.username from Spring configuration.") String user;
    private int count(Integer value) { return value == null ? (all ? 10 : 0) : value; }
    public Integer call() throws Exception {
        boolean dataSelected = all || customers != null || accounts != null || relationships != null || transactions != null;
        if (!dataSelected && batchDate == null)
            throw new IllegalArgumentException("Select --batch-date, --all or at least one data record type.");
        if (dataSelected && date == null)
            throw new IllegalArgumentException("--date is required when selecting data record types.");
        var request = dataSelected ? new Generate.Request(date, count(customers), count(accounts),
            count(relationships), count(transactions), seed) : null;
        if(maxPending<1) throw new IllegalArgumentException("max-pending must be positive");
        if (batchDate != null) {
            Records.date(batchDate.toString());
            if (dataSelected && !batchDate.equals(date))
                throw new IllegalArgumentException("--batch-date must equal --date when generating data");
        }
        var topics=banking.KafkaGenerator.topics();
        if(overrides!=null) for(var entry:overrides.entrySet()) {
            if(!topics.containsKey(entry.getKey()) || entry.getValue().isBlank()) throw new IllegalArgumentException("Invalid topic override: "+entry.getKey());
            topics.put(entry.getKey(),entry.getValue());
        }
        if(brokers==null) brokers=System.getenv().getOrDefault("CAMEL_BROKERS","localhost:19092");
        if (!dataSelected) {
            publish(source -> {}, topics);
            System.out.printf("Published batch-date %s%n", batchDate);
            return 0;
        }
        try (var context = GeneratorConfiguration.open()) {
            Config config = context.getBean(Config.class);
            var database = config.database();
            try (var db = DriverManager.getConnection(url == null ? database.url() : url,
                    user == null ? database.username() : user, database.password())) {
                var source = new Generate().prepare(db, request);
                publish(source, topics);
            }
        }
        System.out.printf("Published for %s: customers=%d accounts=%d relationships=%d transactions=%d%n",
            date, request.customers(),request.accounts(),request.relationships(),request.transactions());
        return 0;
    }
    Producer<String,String> createProducer() {
        return new KafkaProducer<>(banking.KafkaGenerator.properties(brokers));
    }
    private void publish(Generate.Source source, Map<String,String> topics) throws Exception {
        try (var producer = createProducer()) {
            new banking.KafkaGenerator().publish(producer,source,batchDate,maxPending,topics);
        }
    }
    public static void main(String[] args) {
        var command = new CommandLine(new KafkaGenerator());
        command.setExecutionExceptionHandler((error, cmd, parsed) -> {
            cmd.getErr().println("Generation failed: " + error.getMessage()); return 1;
        });
        System.exit(command.execute(args));
    }
}
