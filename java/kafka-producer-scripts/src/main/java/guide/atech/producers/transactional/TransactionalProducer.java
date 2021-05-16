package guide.atech.producers.transactional;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Preparation
 * - Creating Topics
 *   - kafka-topics.sh --create --zookeeper localhost:2181 --topic transactional-producer-1 --partitions 5 --replication-factor 3 --config min.insync.replicas=2
 *   - kafka-topics.sh --create --zookeeper localhost:2181 --topic transactional-producer-2 --partitions 5 --replication-factor 3 --config min.insync.replicas=2
 *
 * Ref [25 Implementing Transaction]
 *  - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15463140#overview
 */
@Slf4j
public class TransactionalProducer {

    private static final String TOPIC_NAME_1 = "transactional-producer-1";
    private static final String TOPIC_NAME_2 = "transactional-producer-2";
    private static final String TRANSACTIONAL_ID = "TransactionalProducer-v1";
    private static final Integer NUM_EVENTS = 2;

    public static void main(String[] args) {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        // Step 1
        producer.initTransactions();

        // Transaction 1 (Commit Scenario)
        // Step 2
        log.info("Start First Transaction ...");
        producer.beginTransaction();
        try {
            for (int i = 0; i < NUM_EVENTS; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME_1, i, "Simple Message-T1-" + i));
                producer.send(new ProducerRecord<>(TOPIC_NAME_2, i, "Simple Message-T1-" + i));
            }

            // Step 3
            log.info("Committing First Transaction");
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            producer.close();

            log.error("Exception Received {}", e.getLocalizedMessage());
        }

        // Transaction 2 (Abort Scenario)
        // Step 2
        log.info("Start Second Transaction ...");
        producer.beginTransaction();
        try {
            for (int i = 0; i < NUM_EVENTS; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME_1, i, "Simple Message-T2-" + i));
                producer.send(new ProducerRecord<>(TOPIC_NAME_2, i, "Simple Message-T2-" + i));
            }

            // Step 3
            log.info("Aborting Second Transaction"); // all the 4 messages that I am sending in second transaction will rollback.
            producer.abortTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            producer.close();

            log.error("Exception Received {}", e.getLocalizedMessage());
        }

        log.info("Finished Sending messages, Closing the Producer");
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, TransactionalProducer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Setting a transaction id is a mandatory requirement for the producer
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        return properties;
    }

}
