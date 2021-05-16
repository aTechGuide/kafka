package guide.atech.producers.partitioner;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *
 * Consumer Command
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic partitioner-producer-topic \
 *  --from-beginning \
 *  --property print.key=true \
 *  --property key.separator="-" \
 *  --partition 0 \
 *  --key-deserializer "org.apache.kafka.common.serialization.IntegerDeserializer" \
 *  --value-deserializer "org.apache.kafka.common.serialization.StringDeserializer"
 *
 * Ref [27 Kafka Producer]
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307222
 */

@Slf4j
public class PartitionerProducer {

    private static final String TOPIC_NAME = "partitioner-producer-topic";

    public static void main(String[] args) {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        log.info("Start sending messages");
        for (int i = 0; i < 6; i++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, i, "Simple Message-" + i);
            producer.send(producerRecord);
        }

        log.info("Finished Sending messages, Closing the Producer");
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, PartitionerProducer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OddEvenPartitioner.class);
        return properties;
    }

}
