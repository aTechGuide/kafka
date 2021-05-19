package guide.atech.kstreams;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class BasicStreams {

    private static final String TOPIC_NAME = "basic-producer-topic";

    public static void main(String[] args) {

        val properties = createProperties();

        // Most of the DSL APIs are available through StreamsBuilder() class.
        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Opening a stream to a source topic
        KStream<Integer, String> kStream = builder.stream(TOPIC_NAME);

        // Step 2: Process the stream
        // Building computational logic.
        kStream.foreach((k, v) -> log.info("Key = {} Value = {}", k, v));

        // Step 3: Create a Topology
        // The Kafka Streams computational logic is known as a Topology.
        // Whatever we defined as computational logic, we can get all that bundled into a Topology object by calling the build() method.
        // So, everything that we have done so far, starting from the builder.stream(), the foreach() call, all that is bundled inside a single object called Topology.
        Topology topology = builder.build();

        // Step 4: Instantiating the KafkaStreams object
        KafkaStreams streams = new KafkaStreams(topology, properties);

        log.info("Starting the Stream");
        streams.start();

        // Step 5: Graceful Shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful Shutdown");
            streams.close();
        }));

    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        // Every Kafka Streams application must be identified by a unique application id.
        // If we are executing multiple instances of our Kafka Streams application, all instances of the same application must have the same application id.
        // That is how Kafka would know that all the instances belong to the same single application.
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, BasicStreams.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        // Serdes is a factory-class that combines a serializer, and a deserializer.
        // A typical Kafka Streams application would be reading data and writing it, internally create a combination of consumer and producer.
        // So, they would need a serializer and a deserializer.
        // Therefore, the Streams API takes a Serdes approach for the Key and the value.
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
