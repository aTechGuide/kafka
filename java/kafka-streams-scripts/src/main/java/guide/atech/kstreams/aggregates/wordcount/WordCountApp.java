package guide.atech.kstreams.aggregates.wordcount;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;

/**
 * Ref
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/14243916#notes
 */
@Slf4j
public class WordCountApp {

    private static final String INPUT_TOPIC_NAME = "word-count-topic";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        // Defining the Processing
        builder
                .<String, String> stream(INPUT_TOPIC_NAME)
                .flatMapValues(v -> Arrays.asList(v.toLowerCase().split(" "))) // Splitting Sentences in words
                .groupBy((k,v) -> v)
                .count()
                .toStream()
                .print(Printed.<String, Long> toSysOut().withLabel("KT1"));

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

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, WordCountApp.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        // This configuration defines the base directory location where the application is going to create our local state store.
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/state-store");

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
