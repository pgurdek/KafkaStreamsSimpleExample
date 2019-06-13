package transformers;

import dto.Purchase;
import dto.PurchaseCounter;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

public class PurchaseCounterTransformer implements ValueTransformer<Purchase, PurchaseCounter> {

    private final String storeName;
    private ProcessorContext contex;
    private KeyValueStore<String,Integer> stateStore;

    public PurchaseCounterTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.contex = processorContext;
        stateStore = (KeyValueStore) this.contex.getStateStore(storeName);
    }

    @Override
    public PurchaseCounter transform(Purchase purchase) {
        int count = 0;
        Integer purchasedItemsSoFar = stateStore.get(purchase.getCustomerId());
        if (purchasedItemsSoFar != null){
            count = purchasedItemsSoFar;
        }
        if(purchase.getPrice() > 5){
            count++;
        }
        PurchaseCounter purchaseCounter  = new PurchaseCounter(count,purchase.getCustomerId());
        stateStore.put(purchase.getCustomerId(),purchaseCounter.getCount());
        return purchaseCounter;
    }

    public PurchaseCounter punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {

    }
}
