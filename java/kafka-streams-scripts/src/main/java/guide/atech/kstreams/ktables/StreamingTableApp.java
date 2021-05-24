package guide.atech.kstreams.ktables;

import guide.atech.kstreams.BasicStreams;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 * Query Server URL: http://localhost:7010/kt01-store/all
 *
 * Sample Data
 * - HDFCBANK:43.430 [Insert Scenario]
 * - TCS:2150.0
 * - KOTAK:2323.0 [Filter Scenario, This value will not be present in KT1]
 * - HDFCBANK:53.430 [Update Scenario]
 * - HDFCBANK: [Delete Scenario]
 */
@Slf4j
public class StreamingTableApp {

    private static final String TOPIC_NAME = "ktable-topic";
    public static final String STORE_NAME = "kt01-store";

    private static final String REGEX = "(?i)HDFCBANK|TCS";

    private static final String QUERY_SERVER_HOST = "localhost:9092";
    private static final Integer QUERY_SERVER_PORT = 7010;

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> kt0 = builder.table(TOPIC_NAME);
        kt0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0"));

        KTable<String, String> kt1 = kt0.filter((k,v) -> k.matches(REGEX) && !v.isEmpty(), Materialized.as(STORE_NAME));
        kt1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        // Querying the State Store
        QueryServer queryServer = new QueryServer(streams, QUERY_SERVER_HOST, QUERY_SERVER_PORT);
        streams.setStateListener((newState, oldState) -> {
            log.info("State Changing to " + newState + " from " + oldState);
            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });

        log.info("Starting the Stream");
        streams.start();

        log.info("Starting the Query Server");
        queryServer.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful Shutdown");
            queryServer.stop();
            streams.close();
        }));

    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, BasicStreams.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        // This configuration defines the base directory location where the application is going to create our local state store.
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/state-store");

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }
}
