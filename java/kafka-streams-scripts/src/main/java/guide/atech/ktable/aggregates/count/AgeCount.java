package guide.atech.ktable.aggregates.count;

import guide.atech.serde.AppSerdes;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 *
 * Create a topic named person-age and send the following messages. Each record shown here is a key/value pair.
 * The key is the name of a person, and the value is the age of the person.
 * - Abdul: 26
 * - Prashant: 41
 * - John: 38
 * - Milli: 26
 *
 * We want to calculate the count() by age.
 * The result is obvious, we have one person of age 41 and another one of age 38, whereas, we have two persons of age 26.
 *
 * The requirement that we have in our hand is an update stream problem because the age of a person is not constant. It will change
 * The person with age 41 will become 42. And in that case, the age for the person should be updated to 42. This update will also change the aggregates,
 * The count for the 41 age should be reduced by one, and at the same time, the count for the age 42 should increase by one
 * So, we need to model the solution using KTable aggregation.
 *
 * Ref - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/14244144#notes
 */

@Slf4j
public class AgeCount {

    private static final String INPUT_TOPIC_NAME = "word-count-topic";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        // Defining the Processing
        builder
                .<String, String> table(INPUT_TOPIC_NAME, Consumed.with(AppSerdes.String(), AppSerdes.String()))
                .groupBy((person,age) -> KeyValue.pair(age, "1"))
                .count()
                .toStream()
                .print(Printed.<String, Long> toSysOut().withLabel("Age Count"));

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

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AgeCount.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/state-store");
        return properties;
    }
}
