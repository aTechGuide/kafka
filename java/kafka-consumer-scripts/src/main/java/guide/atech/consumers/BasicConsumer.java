package guide.atech.consumers;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class BasicConsumer {

    private static final String GROUP_ID = "BasicConsumer-v1";
    private static final String TOPIC_NAME = "basic-consumer-topic";

    public static void main(String[] args) {

        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<Integer, String> consumerRecord : records) {
                log.info("Record Key = {}", consumerRecord.key());
                log.info("Record Value = {}", consumerRecord.value());
            }
        }

    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        // CLIENT_ID_CONFIG is a simple string that is passed to the Kafka server.
        // The purpose of the client id is to track the source of the message.
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, BasicConsumer.class.getName());

        // The producer will use this information for establishing the initial connection to the Kafka cluster.
        // Once connected, the Kafka producer will automatically query for the metadata and discover the full list of Kafka brokers in the cluster.
        // That means we do not need to supply a complete list of Kafka brokers as a bootstrap configuration.
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
