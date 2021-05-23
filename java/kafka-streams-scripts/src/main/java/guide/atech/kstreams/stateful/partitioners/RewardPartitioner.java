package guide.atech.kstreams.stateful.partitioners;

import guide.atech.schema.json.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardPartitioner implements StreamPartitioner<Integer, PosInvoice> {
    @Override
    public Integer partition(String topic, Integer key, PosInvoice value, int numPartitions) {
        return value.getCustomCardNo().hashCode() % numPartitions;
    }
}
