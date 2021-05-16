package guide.atech.producers.sync;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Ref [27 Kafka Producer]
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307222
 */

@Slf4j
public class SyncProducer {

    private static final String TOPIC_NAME = "sync-producer-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        log.info("Start sending messages");
        try {

            for (int i = 0; i < 2; i++) {
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, i, "Simple Message-" + i);
                RecordMetadata metadata = producer.send(producerRecord).get();

                log.info("Metadata topic={} partition={} ", metadata.topic(), metadata.partition());
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception while sending message {}", e.getLocalizedMessage());
            throw e;
        }


        log.info("Finished Sending messages, Closing the Producer");
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, SyncProducer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
