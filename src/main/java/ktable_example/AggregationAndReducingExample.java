package ktable_example;

import json_serializers.JsonDeserializerGson;
import json_serializers.JsonSerlizerGson;
import ktable_example.dto.ShareVolume;
import ktable_example.dto.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class AggregationAndReducingExample {
    private static Logger LOG = LoggerFactory.getLogger(AggregationAndReducingExample.class);

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";


    public static void main(String[] args) throws InterruptedException {
        JsonSerlizerGson<StockTransaction> stockTransactionJsonSerlizerGson = new JsonSerlizerGson<>();
        JsonSerlizerGson<ShareVolume> shareVolumeJsonSerlizerGson = new JsonSerlizerGson<>();
        JsonDeserializerGson<StockTransaction> stockTransactionJsonDeSerlizerGson = new JsonDeserializerGson<>(StockTransaction.class);
        JsonDeserializerGson<ShareVolume> shareVolumeJsonDeserializerGson = new JsonDeserializerGson<>(ShareVolume.class);
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionJsonSerlizerGson, stockTransactionJsonDeSerlizerGson);
        Serde<ShareVolume> shareVolumeSerde = Serdes.serdeFrom(shareVolumeJsonSerlizerGson, shareVolumeJsonDeserializerGson);


        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, ShareVolume> shareVolumeKTable = builder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST))
                .mapValues(stockTransaction -> ShareVolume.newBuilder(stockTransaction).build())
                .groupBy((k, v) -> v.getSymbol(), Grouped.with("GroupName",stringSerde,shareVolumeSerde))
                .reduce(ShareVolume::sum);

        shareVolumeKTable.toStream().print(Printed.<String,ShareVolume>toSysOut().withLabel("K-Table"));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        LOG.info("First Reduction and Aggregation Example Application Started");
        kafkaStreams.start();
        Thread.sleep(100000);
        LOG.info("Shutting down the Reduction and Aggregation Example Application now");
        kafkaStreams.close();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
