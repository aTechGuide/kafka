package guide.atech.producers;

import guide.atech.producers.builder.POSBuilder;
import guide.atech.schema.avro.PosInvoice;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import java.util.Properties;

@Slf4j
public class BasicProducer {

    private static final String TOPIC_NAME = "basic-avro-producer-topic";

    public static void main(String[] args) {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, PosInvoice> producer = new KafkaProducer<>(properties);

        log.info("Start sending messages");
        for (int i = 0; i < 2; i++) {
            /*
                Creating Record
                - A Kafka message must have a key/value structure.
                  - We can have a null key, but the message is still structured as a key/value pair.
                - As Kafka messages are sent over the network, so the key and the value must be serialized into BYTES before they are streamed over the network.
            */
            ProducerRecord<Integer, PosInvoice> producerRecord = new ProducerRecord<>(TOPIC_NAME, i, POSBuilder.nextInvoice());
            producer.send(producerRecord);
        }

        log.info("Finished Sending messages, Closing the Producer");
        // Producer consists of some buffer space and background I/O thread.
        // If we do not close the producer after sending all the required messages, we will leak the resources created by the producer.
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, BasicProducer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return properties;
    }

}
