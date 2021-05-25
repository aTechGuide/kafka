package guide.atech.kstreams.stateful;

import guide.atech.serde.AppSerdes;
import guide.atech.kstreams.stateful.partitioners.RewardPartitioner;
import guide.atech.kstreams.stateful.transformers.RewardsTransformer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Ref
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/14243916#notes
 */
@Slf4j
public class RewardsApp {

    private static final String INPUT_TOPIC_NAME = "basic-producer-topic";
    private static final String NOTIFICATION_TOPIC_NAME = "notification-topic";
    private static final String TEMP_TOPIC_NAME = "temp-topic";
    public static final String REWARDS_STORE_NAME = "CustomerRewardsStore";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        // Adding the Store to the Stream Builder
        StoreBuilder<KeyValueStore<String, Double>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(REWARDS_STORE_NAME),
                AppSerdes.String(),
                AppSerdes.Double()
        );

        builder.addStateStore(storeBuilder);

        // Defining the Processing
        builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(AppSerdes.Integer(), AppSerdes.PosInvoice()))
                .filter((key, value) -> value.getCustomerType().equalsIgnoreCase("PRIME"))
                .through(TEMP_TOPIC_NAME, Produced.with(AppSerdes.Integer(), AppSerdes.PosInvoice(), new RewardPartitioner())) // We want data of same customer to go to same Partition
                .transformValues(RewardsTransformer::new, REWARDS_STORE_NAME)
                .to(NOTIFICATION_TOPIC_NAME, Produced.with(AppSerdes.Integer(), AppSerdes.Notification()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        log.info("Starting the Stream");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful Shutdown");
            streams.close();
        }));

    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, RewardsApp.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        return properties;
    }
}
