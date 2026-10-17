package banking;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;

public final class BankingRoutes extends RouteBuilder {
    private final Settings settings;
    private final Store store;
    private final FilesBridge files;
    private final AtomicReference<Throwable> failure=new AtomicReference<>();
    public BankingRoutes(Settings settings) { this.settings=settings; store=new Store(settings); files=new FilesBridge(store,settings); }
    @Override public void configure() {
        // Waiting for the initial date is expected. Other failures latch every route until restart.
        onException(Store.AwaitingClock.class).maximumRedeliveries(0).logExhausted(false).handled(false);
        onException(Exception.class).maximumRedeliveries(0).logExhausted(false).process(e -> {
            Throwable cause=e.getProperty(Exchange.EXCEPTION_CAUGHT,Throwable.class);
            if (failure.compareAndSet(null,cause)) log.error("Camel banking routes blocked; correct the problem and restart route={} topic={} partition={} offset={}",
                e.getFromRouteId(),e.getMessage().getHeader(KafkaConstants.TOPIC),
                e.getMessage().getHeader(KafkaConstants.PARTITION),e.getMessage().getHeader(KafkaConstants.OFFSET),cause);
        }).handled(false);
        from(consumer(Topics.DATE)).routeId("batch-date").process(this::ingest);
        for (String kind : Topics.KINDS)
            from(consumer("input."+kind)).routeId("stage-"+kind).process(this::ingest);
        from("timer:closed-batches?period=1000&delay=1000").routeId("extract-closed-batches")
            .filter(e -> failure.get() == null)
            .process(e -> { requireHealthy(); files.extractNextClosedBatchIfAvailable(); });
        from("timer:balance-files?period=1000&delay=1000").routeId("publish-balances")
            .filter(e -> failure.get() == null)
            .process(e -> {
                requireHealthy();
                try (ProducerTemplate producer=e.getContext().createProducerTemplate()) {
                    files.publishPendingBalanceFiles((key,json) -> producer.sendBodyAndHeader(
                        "direct:publish-balance",json,KafkaConstants.KEY,key));
                }
            });
        from("direct:publish-balance").routeId("balance-producer")
            .to("kafka:"+Topics.BALANCE+"?brokers="+settings.brokers()+"&requestRequiredAcks=all&enableIdempotence=true&synchronous=true");
    }
    private String consumer(String topic) {
        return "kafka:"+topic+"?brokers="+settings.brokers()+"&groupId="+settings.group()
            +"&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true&breakOnFirstError=true"
            +"&maxPollRecords=1&consumersCount=1&pollTimeoutMs=1000&additionalProperties.allow.auto.create.topics=false";
    }
    private void requireHealthy() { if (failure.get() != null) throw new IllegalStateException("Routes blocked until restart",failure.get()); }
    private void ingest(Exchange e) throws Exception {
        requireHealthy();
        var m=e.getMessage();
        var date=store.accept(new Store.Source(m.getHeader(KafkaConstants.TOPIC,String.class),
            m.getHeader(KafkaConstants.PARTITION,Integer.class),m.getHeader(KafkaConstants.OFFSET,Long.class),
            m.getHeader(KafkaConstants.KEY,String.class),m.getBody(String.class)));
        // Must remain on the Kafka consumer thread; never put an async EIP before this commit.
        m.getHeader(KafkaConstants.MANUAL_COMMIT,KafkaManualCommit.class).commit();
        log.debug("Kafka offset acknowledged batchDate={} topic={} partition={} offset={}",date,
            m.getHeader(KafkaConstants.TOPIC),m.getHeader(KafkaConstants.PARTITION),m.getHeader(KafkaConstants.OFFSET));
    }
}
