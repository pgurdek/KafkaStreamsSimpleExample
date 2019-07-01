import dto.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimeStamp) {
        Purchase purchaseTranscation = (Purchase) consumerRecord.value();
        return purchaseTranscation.getPurchaseDate().getTime();
    }
}
