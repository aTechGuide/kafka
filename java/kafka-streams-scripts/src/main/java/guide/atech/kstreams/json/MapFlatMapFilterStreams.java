package guide.atech.kstreams.json;

import guide.atech.kstreams.BasicStreams;
import guide.atech.kstreams.json.types.Message;
import guide.atech.kstreams.json.types.SubMessage;
import guide.atech.kstreams.serde.AppSerdes;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class MapFlatMapFilterStreams {

    private static final String INPUT_TOPIC_NAME = "json-topic-v1";
    private static final String OUTPUT_TOPIC_NAME_V1 = "output-topic-v1";
    private static final String OUTPUT_TOPIC_NAME_V2 = "output-topic-v2";
    private static final String OUTPUT_TOPIC_NAME_V3 = "output-topic-v3";


    public static void main(String[] args) {

        val properties = createProperties();


        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Message> ks0 = builder.stream(INPUT_TOPIC_NAME,
                Consumed.with(AppSerdes.Integer(), AppSerdes.Message()));

        // Filter Operation
        ks0.filter((k,v) -> v.getMessageCode() > 2)
                .to(OUTPUT_TOPIC_NAME_V1,
                        Produced.with(AppSerdes.Integer(), AppSerdes.Message()));

        // Map Operation
        ks0.mapValues(Message::getMessageDescription)
                .to(OUTPUT_TOPIC_NAME_V2,
                        Produced.with(AppSerdes.Integer(), AppSerdes.String()));

        // flatMap Operation
        ks0.flatMapValues(v -> v.getSubMessages().stream().map(SubMessage::getSubMessageCode).collect(Collectors.toList()))
                .to(OUTPUT_TOPIC_NAME_V3,
                        Produced.with(AppSerdes.Integer(), AppSerdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        log.info("Starting the Stream");
        streams.start();
        addGracefulShutdown(streams);
    }

    private static void addGracefulShutdown(KafkaStreams streams) {
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
