package guide.atech.producers;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

@Slf4j
public class BasicProducer {

    private static final String TOPIC_NAME = "basic-producer-topic";

    public static void main(String[] args) {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        log.info("Start sending messages");
        for (int i = 0; i < 10; i++) {
            /*
                Creating Record
                - A Kafka message must have a key/value structure.
                  - We can have a null key, but the message is still structured as a key/value pair.
                - As Kafka messages are sent over the network, so the key and the value must be serialized into BYTES before they are streamed over the network.
            */
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, i, "Simple Message-" + i);
            producer.send(producerRecord);
        }

        log.info("Finished Sending messages, Closing the Producer");
        // Producer consists of some buffer space and background I/O thread.
        // If we do not close the producer after sending all the required messages, we will leak the resources created by the producer.
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        // CLIENT_ID_CONFIG is a simple string that is passed to the Kafka server.
        // The purpose of the client id is to track the source of the message.
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, BasicProducer.class.getName());

        // The producer will use this information for establishing the initial connection to the Kafka cluster.
        // Once connected, the Kafka producer will automatically query for the metadata and discover the full list of Kafka brokers in the cluster.
        // That means we do not need to supply a complete list of Kafka brokers as a bootstrap configuration.
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
