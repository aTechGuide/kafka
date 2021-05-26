package guide.atech.kstreams.aggregates.aggregate;

import guide.atech.serde.AppSerdes;
import guide.atech.schema.json.employee.DepartmentAggregate;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

/**
 * Ref
 * - https://www.udemy.com/course/kafka-streams-real-time-stream-processing-master-class/learn/lecture/14244122
 */
@Slf4j
public class AverageSalaryByDepartmentApp {

    private static final String INPUT_TOPIC_NAME = "basic-producer-topic";
    private static final String STATE_STORE_NAME = "average-store";

    public static void main(String[] args) {

        val properties = createProperties();

        StreamsBuilder builder = new StreamsBuilder();

        // Defining the Processing
        builder
                .stream(INPUT_TOPIC_NAME, Consumed.with(AppSerdes.String(), AppSerdes.Employee()))
                // We want to change the key from employee id to the department
                .groupBy((k, v) -> v.getDepartment(), Grouped.with(AppSerdes.String(), AppSerdes.Employee()))
                .aggregate(
                        // Initializer
                        () -> new DepartmentAggregate()
                        .withEmployeeCount(0)
                        .withTotalSalary(0)
                        .withAvgSalary(0.0),
                        // Aggregator
                        (k,v,aggValue) -> new DepartmentAggregate()
                        .withEmployeeCount(aggValue.getEmployeeCount() + 1)
                        .withTotalSalary(aggValue.getTotalSalary() + v.getSalary())
                        .withAvgSalary((aggValue.getTotalSalary() + v.getSalary()) / (aggValue.getEmployeeCount() + 1D) ),
                        // Serializer
                        Materialized.<String, DepartmentAggregate, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME)
                        .withKeySerde(AppSerdes.String())
                        .withValueSerde(AppSerdes.DepartmentAggregate())
                )
                .toStream()
                .print(Printed.<String, DepartmentAggregate>toSysOut().withLabel("Department Salary Average"));

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

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AverageSalaryByDepartmentApp.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

        properties.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/state-store");
        return properties;
    }
}
