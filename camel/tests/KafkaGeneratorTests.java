///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21
//DEPS org.apache.kafka:kafka-clients:4.1.1
//DEPS com.fasterxml.jackson.core:jackson-databind:2.18.3
//DEPS org.junit.platform:junit-platform-console-standalone:1.11.4
//SOURCES ../src/banking/KafkaGenerator.java ../src/banking/Records.java ../src/banking/Topics.java
//SOURCES ../../corebank/src/corebank/generator/Generate.java ../../corebank/src/corebank/generator/GeneratedRecord.java

import banking.*;
import corebank.generator.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaGeneratorTests {
    static GeneratedRecord record() { return new GeneratedRecord("transaction","9223372036854775807",
        List.of("transactionId","accountId","type","amount"),List.of(Long.MAX_VALUE,1L,"CR",123L),List.of(19,19,6,20)); }
    static MockProducer<String,String> producer(boolean auto) {
        return new MockProducer<>(auto,null,new StringSerializer(),new StringSerializer());
    }
    @Test void preservesKeysExactLongsAndFormats() throws Exception {
        try(var p=producer(true)) {
            new KafkaGenerator().publish(p,s -> { for(int i=0;i<25;i++) s.accept(record()); },null,3,KafkaGenerator.topics());
            assertEquals(25,p.history().size());
            for(var sent:p.history()) {
                assertEquals("input.transaction",sent.topic());
                assertEquals(record().key(),sent.key());
                assertEquals(record().line(),Records.decode("transaction",sent.value()).line());
            }
        }
    }
    @Test void waitsForControlAndFinalDataAcknowledgements() throws Exception {
        try(var p=producer(false); var executor=Executors.newSingleThreadExecutor()) {
            var started=new CountDownLatch(1);
            Future<?> task=executor.submit(() -> {
                try { new KafkaGenerator().publish(p,s -> { started.countDown(); s.accept(record()); },LocalDate.of(2026,10,8),2,KafkaGenerator.topics()); }
                catch(Exception e) { throw new RuntimeException(e); }
            });
            awaitHistory(p,1);
            assertEquals("system.batch-date",p.history().getFirst().topic());
            assertEquals("clock",p.history().getFirst().key());
            assertEquals("2026-10-08",p.history().getFirst().value());
            assertEquals(1,started.getCount()); assertFalse(task.isDone());
            p.completeNext(); assertTrue(started.await(2,TimeUnit.SECONDS));
            awaitHistory(p,2); assertFalse(task.isDone());
            p.completeNext(); task.get(2,TimeUnit.SECONDS);
        }
    }
    @Test void failedControlPreventsGeneration() throws Exception {
        try(var p=producer(false); var executor=Executors.newSingleThreadExecutor()) {
            var generated=new AtomicBoolean();
            var task=executor.submit(() -> {
                assertThrows(ExecutionException.class,() -> new KafkaGenerator().publish(p,s -> generated.set(true),LocalDate.of(2026,10,8),1,KafkaGenerator.topics()));
            });
            awaitHistory(p,1); p.errorNext(new IllegalStateException("broker rejected"));
            task.get(2,TimeUnit.SECONDS); assertFalse(generated.get()); assertEquals(1,p.history().size());
        }
    }
    @Test void boundedSendsAndDataFailurePropagate() throws Exception {
        try(var p=producer(false); var executor=Executors.newSingleThreadExecutor()) {
            var task=executor.submit(() -> {
                assertThrows(ExecutionException.class,() -> new KafkaGenerator().publish(p,s -> {for(int i=0;i<10;i++) s.accept(record());},null,2,KafkaGenerator.topics()));
            });
            awaitHistory(p,2); assertEquals(2,p.history().size());
            p.errorNext(new IllegalStateException("failed data")); task.get(2,TimeUnit.SECONDS);
            assertEquals(2,p.history().size());
        }
    }
    @Test void overridesAndValidation() throws Exception {
        try(var p=producer(true)) {
            var topics=KafkaGenerator.topics(); topics.put("transaction","custom.transactions");
            new KafkaGenerator().publish(p,s -> s.accept(record()),null,1,topics);
            assertEquals("custom.transactions",p.history().getFirst().topic());
            assertThrows(IllegalArgumentException.class,() -> new KafkaGenerator().publish(p,s -> {},null,0,topics));
            assertEquals("all",KafkaGenerator.properties("localhost:19092").getProperty("acks"));
            assertEquals("true",KafkaGenerator.properties("localhost:19092").getProperty("enable.idempotence"));
        }
    }
    @Test
    @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="CAMEL_TEST_BROKERS",matches=".+")
    void realBrokerReceivesAcknowledgedControlThenExactCount() throws Exception {
        String brokers=System.getenv("CAMEL_TEST_BROKERS");
        String topic="generator-test-"+UUID.randomUUID();
        try(var admin=org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers",brokers))) {
            admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(topic,1,(short)1))).all().get(20,TimeUnit.SECONDS);
            try {
                var topics=KafkaGenerator.topics();
                topics.put("batch-date",topic); topics.put("transaction",topic);
                try(var p=new KafkaProducer<String,String>(KafkaGenerator.properties(brokers))) {
                    new KafkaGenerator().publish(p,s -> {for(int n=0;n<25;n++) s.accept(record());},LocalDate.of(2026,10,8),3,topics);
                }
                var config=new Properties();
                config.put("bootstrap.servers",brokers);
                config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                config.put("enable.auto.commit","false");
                try(var consumer=new org.apache.kafka.clients.consumer.KafkaConsumer<String,String>(config)) {
                    var partition=new org.apache.kafka.common.TopicPartition(topic,0);
                    consumer.assign(List.of(partition)); consumer.seekToBeginning(List.of(partition));
                    assertEquals(26L,consumer.endOffsets(List.of(partition)).get(partition));
                    int count=0; long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
                    while(count<26 && System.nanoTime()<deadline) {
                        for(var received:consumer.poll(java.time.Duration.ofMillis(200))) {
                            if(count==0) { assertEquals("clock",received.key()); assertEquals("2026-10-08",received.value()); }
                            else { assertEquals(record().key(),received.key()); assertEquals(record().line(),Records.decode("transaction",received.value()).line()); }
                            count++;
                        }
                    }
                    assertEquals(26,count);
                }
            } finally { admin.deleteTopics(List.of(topic)).all().get(20,TimeUnit.SECONDS); }
        }
    }
    static void awaitHistory(MockProducer<String,String> p,int count) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
        while(p.history().size()<count && System.nanoTime()<deadline) Thread.sleep(5);
        assertEquals(count,p.history().size());
    }
    public static void main(String[] args) { org.junit.platform.console.ConsoleLauncher.main("execute","--select-class=KafkaGeneratorTests","--fail-if-no-tests"); }
}
