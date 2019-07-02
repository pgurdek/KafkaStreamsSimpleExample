package kstreams_example;

import kstreams_example.dto.Purchase;
import kstreams_example.dto.PurchaseCounter;
import kstreams_example.dto.PurchasePattern;
import kstreams_example.dto.RewardAccumulator;
import kstreams_example.joins.CorrelatedPurchase;
import kstreams_example.joins.PurchaseJoiner;
import json_serializers.JsonDeserializerGson;
import json_serializers.JsonSerlizerGson;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kstreams_example.streampartitioners.RewardsStreamPartitioner;
import kstreams_example.transformers.PurchaseCounterTransformer;
import kstreams_example.transformers.PurchaseRewardTransformer;

import java.time.Duration;
import java.util.Properties;

public class Zmart {
    private static final Logger LOG = LoggerFactory.getLogger(Zmart.class);

    public static void main(String[] args) throws InterruptedException {

//
//        String purchaseJson = "{\n" +
//                "\"firstName\": \"Piotr\",\n" +
//                "\"lastName\": \"Gurdek\",\n" +
//                "\"customerId\": \"pgurde\",\n" +
//                "\"creditCardNumber\": \"2040-2200-5000-1212\",\n" +
//                "\"itemPurchased\": \"PC new Generation\",\n" +
//                "\"quantity\": 2,\n" +
//                "\"price\": 7000.00,\n" +
//                "\"purchaseDate\": \"2012-04-23T18:25:43.511Z\",\n" +
//                "\"zipCode\": \"340-100\",\n" +
//                "\"department\": \"IT\",\n" +
//                "\"employeeId\": \"50\",\n" +
//                "\"storeId\": \"40\"\n" +
//                "}";

        JsonSerlizerGson<Purchase> purchaseJsonSerializer = new JsonSerlizerGson<>();
        JsonSerlizerGson<PurchaseCounter> purchaseCounterJsonSerlizerGson = new JsonSerlizerGson<>();
        JsonSerlizerGson<PurchasePattern> purchasePatternJsonSerializer = new JsonSerlizerGson<>();
        JsonSerlizerGson<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerlizerGson<>();

        JsonDeserializerGson<Purchase> purchaseJsonDeserializer = new JsonDeserializerGson<>(Purchase.class);
        JsonDeserializerGson<PurchaseCounter> purchaseCounterJsonDeserializerGson = new JsonDeserializerGson<>(PurchaseCounter.class);
        JsonDeserializerGson<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializerGson<>(PurchasePattern.class);
        JsonDeserializerGson<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializerGson<>(RewardAccumulator.class);

//
//        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
//        JsonSerializer<PurchasePattern> purchasePatternJsonSerializer = new JsonSerializer<>();
//        JsonSerializer<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerializer<>();
//
//        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
//        JsonDeserializer<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializer<>(PurchasePattern.class);
//        JsonDeserializer<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializer<>(RewardAccumulator.class);


        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);
        Serde<PurchasePattern> purchasePatternsSerde = Serdes.serdeFrom(purchasePatternJsonSerializer, purchasePatternJsonDeserializer);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorJsonSerializer, rewardAccumulatorJsonDeserializer);
        Serde<PurchaseCounter> purchaseCounterSerde = Serdes.serdeFrom(purchaseCounterJsonSerlizerGson, purchaseCounterJsonDeserializerGson);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, "1000");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Purchase> purchaseKStream = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> maskedPurchaseStream = purchaseKStream.mapValues(purchase -> Purchase.builder(purchase).maskCreditCard().build());

        // Patterns stream handler
        KStream<String, PurchasePattern> stringPurchasePatternKStream = maskedPurchaseStream.mapValues(maskedPurchase -> PurchasePattern.builder(maskedPurchase).build());
        stringPurchasePatternKStream.to("patterns", Produced.with(stringSerde, purchasePatternsSerde));

//        // adding State to processor
        String rewardsStateStoreName = "rewardsPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());

        builder.addStateStore(storeBuilder);
        //  NOTE: method through() does not create internal topic , you need to pre-create it.
        KStream<String, Purchase> transByCustomerStream = purchaseKStream.through( "customer_transactions", Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        // NOTE THAT YOU NEED TO state twice store name to ensure access
        KStream<String, RewardAccumulator> statefulRewardAccumulator = transByCustomerStream.transformValues(() ->  new PurchaseRewardTransformer(rewardsStateStoreName),
                rewardsStateStoreName);

        statefulRewardAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));


        // Another stateful stream processor

        String purchaseCounterStoreName = "purchaseCounterStore";
        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(purchaseCounterStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier, Serdes.String(), Serdes.Integer());

        builder.addStateStore(keyValueStoreStoreBuilder);

        // IMPORTANT IF YOU HAVE MANY PARTITIONS YOU NEED TO re-partition it via key/value like in example above or you need ot have only one partition to have it work, otherwise
        // it will count it  on each partition separately
        KStream<String, PurchaseCounter> stringPurchaseCounterKStream = maskedPurchaseStream.transformValues(() -> new PurchaseCounterTransformer(purchaseCounterStoreName),purchaseCounterStoreName);
        stringPurchaseCounterKStream.to("purchaseCounter",Produced.with(stringSerde,purchaseCounterSerde));

//
        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();
        KStream<Long, Purchase> longPurchaseKStream = maskedPurchaseStream.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey);

        longPurchaseKStream.to("purchases", Produced.with(Serdes.serdeFrom(Long.class), purchaseSerde));


//        stringPurchasePatternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchasePatterns"));
        statefulRewardAccumulator.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewardAccumulator"));
//        longPurchaseKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
        stringPurchaseCounterKStream.print(Printed.<String,PurchaseCounter>toSysOut().withLabel("Purchase Counter"));


        // Move purchases depending on departments name to different topics

        Predicate<String, Purchase> coffeePredicate = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> electronicsPredicate = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");


        /// Purchase Join
        int coffee = 0;
        int electronics = 1;
        KStream<String, Purchase>[] kstreamByDept  = maskedPurchaseStream.selectKey((k,v) -> v.getCustomerId()).branch(coffeePredicate, electronicsPredicate);
        ValueJoiner<Purchase,Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMintueWindows = JoinWindows.of(Duration.ofMinutes(20L));
        KStream<String, Purchase> coffeeKStream = kstreamByDept[coffee];
        coffeeKStream.filter(coffeePredicate).to("coffee",Produced.with(stringSerde,purchaseSerde));
        KStream<String, Purchase> electronicsKStream = kstreamByDept[electronics];
        electronicsKStream.filter(electronicsPredicate).to("electronics",Produced.with(stringSerde,purchaseSerde));

        KStream<String, CorrelatedPurchase> joinedKStream = coffeeKStream.outerJoin(electronicsKStream, purchaseJoiner, twentyMintueWindows, Joined.with(stringSerde, purchaseSerde, purchaseSerde));
//        KStream<String, CorrelatedPurchase> joinedKStream = coffeeKStream.join(electronicsKStream, purchaseJoiner, twentyMintueWindows, Joined.with(stringSerde, purchaseSerde, purchaseSerde));
//        KStream<String, CorrelatedPurchase> joinedKStream = coffeeKStream.join(electronicsKStream, purchaseJoiner, twentyMintueWindows, Joined.with(stringSerde, purchaseSerde, purchaseSerde));

        coffeeKStream.print(Printed.<String,Purchase>toSysOut().withLabel("Coffe"));
        electronicsKStream.print(Printed.<String,Purchase>toSysOut().withLabel("Electronics"));
        joinedKStream.print(Printed.<String,CorrelatedPurchase>toSysOut().withLabel("CorrelatedPurchase"));
        //
        //outside kafka
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(),
                        purchase.getEmployeeId(), purchase.getItemPurchased());

        Predicate<String,Purchase> singleEmploy = (key,purchase) -> purchase.getEmployeeId().equalsIgnoreCase("50");
        maskedPurchaseStream
                .filter(singleEmploy)
                .foreach(purchaseForeachAction);


        //
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.cleanUp();
        LOG.info("kstreams_example.Zmart App Started");
        kafkaStreams.start();
        Thread.sleep(135000);
        LOG.info("Shutting down the kstreams_example.Zmart APP now");
        kafkaStreams.close();
    }

}
