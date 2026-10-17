///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.postgresql:postgresql
//DEPS org.springframework.boot:spring-boot-starter
//DEPS info.picocli:picocli:4.7.7
//SOURCES corebank/generator/Generate.java corebank/generator/GeneratedRecord.java corebank/generator/FileGenerator.java corebank/generator/BatchGenerator.java corebank/generator/GeneratorConfiguration.java corebank/Config.java
//FILES application.yaml=../application.yaml
//FILES generator/firstnames.txt=resources/firstnames.txt
//FILES generator/lastnames.txt=resources/lastnames.txt
//FILES generator/streets.txt=resources/streets.txt

import corebank.generator.Generate;
import corebank.generator.GeneratorConfiguration;
import corebank.Config;
import picocli.CommandLine;
import picocli.CommandLine.*;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.util.concurrent.Callable;

@Command(name="file-generator", mixinStandardHelpOptions=true,
    description="Generate four dated fixed-width input files using a read-only database snapshot.")
public class FileGenerator implements Callable<Integer> {
    @Option(names="--date", required=true, description="Business date (YYYY-MM-DD).") LocalDate date;
    @Option(names="--all", description="Default all four counts to 10; explicit counts override.") boolean all;
    @Option(names="--customers", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer customers;
    @Option(names="--accounts", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer accounts;
    @Option(names="--relationships", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer relationships;
    @Option(names="--transactions", arity="0..1", fallbackValue="10", description="Record count; 10 when selected without a value, otherwise omitted types are empty.") Integer transactions;
    @Option(names="--output", description="Override corebank.input-directory from Spring configuration.") Path output;
    @Option(names="--seed", defaultValue="42", description="Random seed (default: ${DEFAULT-VALUE}).") long seed;
    @Option(names="--db-url", description="Override corebank.database.url from Spring configuration.") String url;
    @Option(names="--db-user", description="Override corebank.database.username from Spring configuration.") String user;
    private int count(Integer value) { return value == null ? (all ? 10 : 0) : value; }
    public Integer call() throws Exception {
        if (!all && customers == null && accounts == null && relationships == null && transactions == null)
            throw new IllegalArgumentException("Select --all or at least one record type.");
        var request = new Generate.Request(date, count(customers), count(accounts),
            count(relationships), count(transactions), seed);
        try (var context = GeneratorConfiguration.open()) {
            Config config = context.getBean(Config.class);
            var database = config.database();
            if (output == null) output = config.inputDirectory();
            try (var db = DriverManager.getConnection(url == null ? database.url() : url,
                    user == null ? database.username() : user, database.password())) {
                new corebank.generator.FileGenerator().generate(db, output, request);
            }
        }
        System.out.printf("Generated %s in %s: customers=%d accounts=%d relationships=%d transactions=%d (seed=%d)%n",
            date, output.toAbsolutePath(), request.customers(), request.accounts(), request.relationships(), request.transactions(), seed);
        return 0;
    }
    public static void main(String[] args) {
        var command = new CommandLine(new FileGenerator());
        command.setExecutionExceptionHandler((error, cmd, parsed) -> {
            cmd.getErr().println("Generation failed: " + error.getMessage()); return 1;
        });
        System.exit(command.execute(args));
    }
}
