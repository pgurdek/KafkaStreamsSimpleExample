import json_serializers.JsonDeserializer;
import json_serializers.JsonDeserializerGson;
import json_serializers.JsonSerializer;
import json_serializers.JsonSerlizerGson;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//        JsonDeserializerGson<Purchase> purchaseJsonSerializer = new JsonDeserializerGson<>();
//        byte[] serialize = purchaseJsonSerializer.serialize(purchaseJson);
//        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);

//            purchaseJsonDeserializer.deserialize()
//        JsonSerlizerGson<Purchase> purchaseJsonSerializer = new JsonSerlizerGson<>();
//        JsonSerlizerGson<PurchasePattern> purchasePatternJsonSerializer = new JsonSerlizerGson<>();
//        JsonSerlizerGson<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerlizerGson<>();
//
//        JsonDeserializerGson<Purchase> purchaseJsonDeserializer = new JsonDeserializerGson<>(Purchase.class);
//        JsonDeserializerGson<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializerGson<>(PurchasePattern.class);
//        JsonDeserializerGson<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializerGson<>(RewardAccumulator.class);


        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        JsonSerializer<PurchasePattern> purchasePatternJsonSerializer = new JsonSerializer<>();
        JsonSerializer<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        JsonDeserializer<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializer<>(PurchasePattern.class);
        JsonDeserializer<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializer<>(RewardAccumulator.class);


        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);
        Serde<PurchasePattern> purchasePatternsSerde = Serdes.serdeFrom(purchasePatternJsonSerializer, purchasePatternJsonDeserializer);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorJsonSerializer, rewardAccumulatorJsonDeserializer);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Purchase> purchaseKStream = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> maskedPurchaseStream = purchaseKStream.mapValues(purchase -> Purchase.builder(purchase).maskCreditCard().build());

        // Patterns stream handler
        KStream<String, PurchasePattern> stringPurchasePatternKStream = maskedPurchaseStream.mapValues(maskedPurchase -> PurchasePattern.builder(maskedPurchase).build());
        stringPurchasePatternKStream.to("patterns", Produced.with(stringSerde, purchasePatternsSerde));

        //Reward Accumulator
        KStream<String, RewardAccumulator> stringRewardAccumulatorKStream = maskedPurchaseStream.mapValues(maskedPurchase -> RewardAccumulator.builder(maskedPurchase).build());
        stringRewardAccumulatorKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        // Store raw masked data
        maskedPurchaseStream.to("purchases", Produced.with(stringSerde, purchaseSerde));


        stringPurchasePatternKStream.print(Printed.<String,PurchasePattern>toSysOut().withLabel("purchasePatterns"));
        stringRewardAccumulatorKStream.print(Printed.<String,RewardAccumulator>toSysOut().withLabel("rewardAccumulator"));
        maskedPurchaseStream.print(Printed.<String,Purchase>toSysOut().withLabel("purchases"));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        LOG.info("Zmart App Started");
        kafkaStreams.start();
        Thread.sleep(135000);
        LOG.info("Shutting down the Zmart APP now");
        kafkaStreams.close();
    }

}
