package guide.atech.producers.multithreaded;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Ref [23 Scaling Kafka Producer]
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307172#overview
 */
@Slf4j
public class DispatcherDemo {

    private static final String TOPIC_NAME = "multi-threaded-producer-topic-topic";
    private static final String[] EVENT_FILES = {"data/data1.csv", "data/data2.csv"};

    public static void main(String[] args) throws InterruptedException {

        val properties = createProperties();
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        Thread[] dispatchers = new Thread[2]; // Reading two files

        log.info("Starting dispatcher threads");

        for (int i = 0; i < 2; i++) {
            dispatchers[i] = new Thread(new Dispatcher(producer, TOPIC_NAME, EVENT_FILES[i]));
            dispatchers[i].start();
        }

        try {
            for (Thread t : dispatchers) t.join();
        } finally {
            producer.close();
            log.info("Finished Dispatcher Demo");
        }

    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, DispatcherDemo.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
