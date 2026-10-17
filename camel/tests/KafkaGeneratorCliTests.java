///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.kafka:kafka-clients:4.1.1
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//SOURCES ../src/banking/KafkaGenerator.java ../src/banking/Records.java ../src/banking/Topics.java
//DEPS org.springframework.boot:spring-boot-dependencies:3.4.4@pom
//DEPS org.postgresql:postgresql
//DEPS org.springframework.boot:spring-boot-starter
//DEPS info.picocli:picocli:4.7.7
//SOURCES ../../corebank/src/corebank/generator/Generate.java ../../corebank/src/corebank/generator/GeneratedRecord.java ../../corebank/src/corebank/generator/FileGenerator.java ../../corebank/src/corebank/generator/BatchGenerator.java ../../corebank/src/corebank/generator/GeneratorConfiguration.java ../../corebank/src/corebank/Config.java
//FILES application.yaml=../../corebank/application.yaml
//FILES generator/firstnames.txt=../../corebank/src/resources/firstnames.txt
//FILES generator/lastnames.txt=../../corebank/src/resources/lastnames.txt
//FILES generator/streets.txt=../../corebank/src/resources/streets.txt

//DEPS org.junit.platform:junit-platform-console-standalone:1.11.4
//SOURCES ../src/KafkaGenerator.java

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

public class KafkaGeneratorCliTests {
    static class Command extends KafkaGenerator {
        final MockProducer<String,String> producer = new MockProducer<>(true,null,new StringSerializer(),new StringSerializer());
        @Override Producer<String,String> createProducer() { return producer; }
    }
    static Command parse(String... args) {
        var command=new Command(); new CommandLine(command).parseArgs(args); return command;
    }
    @Test void batchDateAlonePublishesWithoutDatabaseOrBusinessDate() throws Exception {
        var command=parse("--batch-date=2026-10-17", "--db-url=jdbc:invalid:no-database");
        assertEquals(0,command.call());
        assertNull(command.date);
        assertEquals(1,command.producer.history().size());
        var sent=command.producer.history().getFirst();
        assertEquals("system.batch-date",sent.topic());
        assertEquals("clock",sent.key()); assertEquals("2026-10-17",sent.value());
        assertTrue(command.producer.closed());
    }
    @Test void standaloneControlHonorsTopicOverrideAndIgnoresUnusedDate() throws Exception {
        var command=parse("--batch-date=2026-10-17","--date=2020-01-01","--topic=batch-date=custom.clock");
        assertEquals(0,command.call());
        assertEquals("custom.clock",command.producer.history().getFirst().topic());
    }
    @Test void dataStillRequiresDateAndMatchingControl() {
        var missing=parse("--batch-date=2026-10-17","--customers=1");
        assertTrue(assertThrows(IllegalArgumentException.class,missing::call).getMessage().contains("--date is required"));
        var mismatch=parse("--date=2026-10-17","--batch-date=2026-10-17","--all");
        assertTrue(assertThrows(IllegalArgumentException.class,mismatch::call).getMessage().contains("minus one day"));
        assertThrows(IllegalArgumentException.class,parse("--date=2026-10-17")::call);
        assertThrows(IllegalArgumentException.class,parse()::call);
    }
    @Test void invalidControlFailsBeforePublication() {
        for(String value:new String[]{"0000-01-01","9999-12-31"}) {
            var command=parse("--batch-date="+value);
            assertThrows(IllegalArgumentException.class,command::call);
            assertTrue(command.producer.history().isEmpty());
        }
    }
    public static void main(String[] args) {
        org.junit.platform.console.ConsoleLauncher.main("execute","--select-class=KafkaGeneratorCliTests","--fail-if-no-tests");
    }
}
