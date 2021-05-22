package guide.atech.kstreams;

import guide.atech.kstreams.serdes.AppSerdes;
import guide.atech.schema.avro.PosInvoice;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class BasicStreams {

    private static final String TOPIC_NAME = "basic-avro-producer-topic";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, PosInvoice> kStream = builder.stream(TOPIC_NAME, Consumed.with(AppSerdes.Integer(), AppSerdes.PosInvoice()));
        kStream.foreach((k, v) -> log.info("Key = {} Value = {}", k, v));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);

        log.info("Starting the Stream");
        streams.start();

        gracefulShutdown(streams);
    }

    private static void gracefulShutdown(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful Shutdown");
            streams.close();
        }));
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, BasicStreams.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        return properties;
    }
}
