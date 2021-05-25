package guide.atech.kstreams.aggregates.reduce;

import guide.atech.serde.AppSerdes;
import guide.atech.schema.json.Notification;
import guide.atech.schema.json.PosInvoice;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * Ref
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/14244114#notes
 */
@Slf4j
public class RewardsApp {

    private static final String INPUT_TOPIC_NAME = "basic-producer-topic";
    private static final String NOTIFICATION_TOPIC_NAME = "notification-topic";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        // Defining the Processing
        builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(AppSerdes.Integer(), AppSerdes.PosInvoice()))
                .filter((key, value) -> value.getCustomerType().equalsIgnoreCase("PRIME"))

                // We will be changing the key to customer-id because we want to compute an aggregate on customer-id.
                // This change in the key will ensure that the stream is automatically repartitioned,
                // and the data for the same customer id remains in a single partition.
                .map((k,v) -> new KeyValue<>(v.getCustomCardNo(), NotificationBuilder.getNotification(v)))

                // Grouping would internally use a temporary state store, and hence, we must specify the appropriate Serdes.
                .groupByKey(Grouped.with(AppSerdes.String(), AppSerdes.Notification()))

                // The aggregateValue is the accumulated value which comes from the state store,
                // and the newValue is the current value of this notification event.
                .reduce((aggValue, newValue) -> {
                    newValue.setTotalLoyaltyPoints(newValue.getEarnedLoyaltyPoints() + aggValue.getTotalLoyaltyPoints());
                    // The returned value goes the state store, which would again come back as aggValue in the next call
                    return newValue;
                })
                .toStream()
                .to(NOTIFICATION_TOPIC_NAME, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        log.info("Starting the Stream");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful Shutdown");
            streams.close();
        }));

    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, RewardsApp.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        return properties;
    }

    public static class NotificationBuilder {

        private NotificationBuilder() {}

        public static final  Double LOYALTY_FACTOR = 0.02;

        public static Notification getNotification(PosInvoice invoice) {
            return new Notification()
                    .withInvoiceNumber(invoice.getInvoiceNumber())
                    .withCustomerCardNo(invoice.getCustomCardNo())
                    .withTotalAmount(invoice.getTotalAmount())
                    .withEarnedLoyaltyPoints(invoice.getTotalAmount() * LOYALTY_FACTOR)
                    .withTotalLoyaltyPoints(invoice.getTotalAmount() * LOYALTY_FACTOR);
        }
    }
}
