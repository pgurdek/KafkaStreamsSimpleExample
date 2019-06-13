package streampartitioners;

import dto.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardsStreamPartitioner implements StreamPartitioner<String,Purchase> {
    @Override
    public Integer partition(String s, String s2, Purchase purchase, int numPartitions) {
        return purchase.getCustomerId().hashCode() % numPartitions;
    }
}
