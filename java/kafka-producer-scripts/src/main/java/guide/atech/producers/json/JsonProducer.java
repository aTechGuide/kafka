package guide.atech.producers.json;

import guide.atech.producers.json.serde.JsonSerializer;
import guide.atech.producers.json.types.Message;
import guide.atech.producers.json.types.SubMessage;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Ref [26 Kafka Producer]
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307196#overview
 */

@Slf4j
public class JsonProducer {

    private static final String TOPIC_NAME = "json-topic-v1";
    private static final Integer NUM_MESSAGES = 5;
    private static final Random random = new Random();

    public static void main(String[] args) {
        log.info("Creating Kafka Producer");

        val properties = createProperties();

        KafkaProducer<Integer, Message> producer = new KafkaProducer<>(properties);

        log.info("Start sending messages");
        for (int i = 0; i < NUM_MESSAGES; i++) {
            ProducerRecord<Integer, Message> producerRecord = new ProducerRecord<>(TOPIC_NAME, i, createMessage(i));
            producer.send(producerRecord);
        }

        log.info("Finished Sending messages, Closing the Producer");
        producer.close();
    }

    private static Message createMessage(int i) {

        Message message = new Message();
        message.setMessageCode(i);
        message.setMessageDescription("Random String-" + random.nextInt(5));
        message.setSubMessages(Arrays.asList(createSubMessage(random.nextInt(10)), createSubMessage(random.nextInt(10))));

        return message;
    }

    private static SubMessage createSubMessage(int i) {
        SubMessage subMessage = new SubMessage();
        subMessage.setSubMessageCode(i);
        return subMessage;
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, JsonProducer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return properties;
    }

}
