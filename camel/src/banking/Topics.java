package banking;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

/** Deliberately one partition: updates to a business key have an unambiguous order. */
public final class Topics {
    public static final String DATE = "system.batch-date";
    public static final String BALANCE = "output.balance";
    public static final List<String> KINDS = List.of("customer", "account", "relationship", "transaction");
    public static final List<String> ALL = List.of(DATE, "input.customer", "input.account", "input.relationship", "input.transaction", BALANCE);
    public static Map<String,String> configuration() {
        return Map.of("cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1");
    }
    public static void provision(String brokers) throws Exception {
        try (var admin = client(brokers)) {
            for (String name : ALL) {
                try { admin.createTopics(List.of(new NewTopic(name, 1, (short)1).configs(configuration()))).all().get(); }
                catch (ExecutionException e) { if (!(e.getCause() instanceof TopicExistsException)) throw e; }
            }
            verify(admin);
        }
    }
    public static void verify(String brokers) throws Exception { try (var admin = client(brokers)) { verify(admin); } }
    private static Admin client(String brokers) {
        return Admin.create(Map.of("bootstrap.servers", brokers, "default.api.timeout.ms", "15000", "request.timeout.ms", "10000"));
    }
    private static void verify(Admin admin) throws Exception {
        var descriptions = admin.describeTopics(ALL).allTopicNames().get();
        var resources = ALL.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC,n)).toList();
        var configs = admin.describeConfigs(resources).all().get();
        for (String name : ALL) {
            var partitions = descriptions.get(name).partitions();
            if (partitions.size() != 1 || partitions.getFirst().replicas().size() != 1)
                throw new IllegalStateException("Expected one partition and one replica for development topic " + name);
            var config = configs.get(new ConfigResource(ConfigResource.Type.TOPIC,name));
            for (var entry : configuration().entrySet())
                if (config.get(entry.getKey()) == null || !entry.getValue().equals(config.get(entry.getKey()).value()))
                    throw new IllegalStateException("Topic configuration mismatch: " + name + " " + entry.getKey());
        }
    }
}
