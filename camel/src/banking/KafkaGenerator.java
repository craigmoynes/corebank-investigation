package banking;

import corebank.generator.Generate;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

/** Caller owns the producer. Successful return means every send was acknowledged. */
public final class KafkaGenerator {
    public static Properties properties(String brokers) {
        var p=new Properties();
        p.put("bootstrap.servers",brokers);
        p.put("key.serializer",StringSerializer.class.getName());
        p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all"); p.put("enable.idempotence","true");
        p.put("max.block.ms","30000"); p.put("delivery.timeout.ms","120000");
        return p;
    }
    public void publish(Producer<String,String> producer, Generate.Source source,
                        LocalDate batchDate, int maxPending, Map<String,String> topics) throws Exception {
        if(maxPending<1) throw new IllegalArgumentException("max-pending must be positive");
        for(String kind:Topics.KINDS) if(topics.get(kind)==null || topics.get(kind).isBlank())
            throw new IllegalArgumentException("Missing topic for "+kind);
        if(topics.get("batch-date")==null || topics.get("batch-date").isBlank())
            throw new IllegalArgumentException("Missing batch-date topic");
        if(batchDate!=null) {
            Records.date(batchDate.toString());
            Records.date(batchDate.plusDays(1).toString());
            producer.send(new ProducerRecord<>(topics.get("batch-date"),0,"clock",batchDate.toString())).get();
        }
        var pending=new ArrayDeque<Future<RecordMetadata>>();
        source.writeTo(record -> {
            if(pending.size()>=maxPending) pending.removeFirst().get();
            String json=Records.JSON.writeValueAsString(record.fields());
            pending.addLast(producer.send(new ProducerRecord<>(topics.get(record.kind()),0,record.key(),json)));
        });
        while(!pending.isEmpty()) pending.removeFirst().get();
        producer.flush();
    }
    public static Map<String,String> topics() {
        var topics=new LinkedHashMap<String,String>();
        for(String kind:Topics.KINDS) topics.put(kind,"input."+kind);
        topics.put("batch-date",Topics.DATE);
        return topics;
    }
}
