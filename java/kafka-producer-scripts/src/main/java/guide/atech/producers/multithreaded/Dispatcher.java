package guide.atech.producers.multithreaded;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Scanner;

/**
 * Ref [23 Scaling Kafka Producer]
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/15307172#overview
 */
@Slf4j
@AllArgsConstructor
public class Dispatcher implements Runnable {

    private final KafkaProducer<Integer, String> producer;
    private final String topicName;
    private final String fileLocation;

    @Override
    public void run() {

        log.info("Start Processing: " + fileLocation);
        int counter = 0;

        File file = new File(fileLocation);
        try(Scanner scanner = new Scanner(file)) {
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
                counter++;
            }

            log.info("Sent " + counter + " messages from " + fileLocation);

        } catch (Exception e) {
            log.error(e.toString());
        }


    }
}
