package guide.atech.producers.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.Map;

/*
    For Even Key, Send to partition 0
    For Odd Key, Send to partition 1
 */
public class OddEvenPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Invoked for each message record and returns the partition number where the message should be delivered

        if ((keyBytes == null) || (!(key instanceof Integer))) {
            throw new InvalidRecordException("Key must be a valid Integer");
        } else if (cluster.partitionsForTopic(topic).size() != 2) {
            throw new InvalidTopicException("Topic must have exactly two partitions");
        }

        return (Integer) key % 2;
    }

    @Override
    public void close() {
        // Called once when the producer destroys the OddEvenPartitioner instance
        // Clean up resources
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Called once when producer instantiates
    }
}
