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

//SOURCES FileGenerator.java

import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Backward-compatible file command; generation lives in corebank.generator.Generate. */
@Command(name="generate", mixinStandardHelpOptions=true,
    description="Generate four dated fixed-width input files using a read-only database snapshot.")
public class Generate extends FileGenerator {
    public static void main(String[] args) {
        var command=new CommandLine(new Generate());
        command.setExecutionExceptionHandler((error,cmd,parsed) -> {
            cmd.getErr().println("Generation failed: "+error.getMessage()); return 1;
        });
        System.exit(command.execute(args));
    }
}
