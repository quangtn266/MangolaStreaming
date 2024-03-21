package com.quangtn.streaming;

import com.quangtn.streaming.aggregations.*;
import com.quangtn.streaming.aggregations.functions.*;
import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class StreamingJob {

    private static final String ROCKS_DB_CHECKPOINT_URI = "file:///data/flink/checkpoints";

    private static final boolean ENABLE_INCREMENTAL_CHECKPOINT = true;

    public static void main(String[] args) throws Exception {
        val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // using rocksdb backend.
        flinkEnv.setStateBackend(new RocksDBStateBackend(ROCKS_DB_CHECKPOINT_URI, ENABLE_INCREMENTAL_CHECKPOINT));

        // Deserializers
        val bidReqGzipJsonDeserializer = new BidReqGzipJsonDeserializer();
        val bidRespGzipJsonDeserializer = new BidRespGzipJsonDeserializer();
        val winNotificationGzipJsonDeserializer = new WinNotificationGzipJsonDeserializer();
        val impressionGzipJsonDeserializer = new ImpressionGzipJsonDeserializer();
        val clickGzipJsonDeserializer = new ClickGzipJsonDeserializer();
        val conversionGzipJsonDeserializer = new ConversionGzipJsonDeserializer();
        val postbackGzipJsonDeserializer = new PostbackGzipJsonDeserializer();

        // Kafka v... is the source of the stream
        val bidReqKafkaConsumer = new FlinkKafkaConsumer011<BidReq>(KafkaTopics.BID_REQ, bidReqGzipJsonDeserializer, kafkaProperties());
        val bidRespKafkaConsumer = new FlinkKafkaConsumer011<BidResp>(KafkaTopics.BID_RESPONSE, bidRespGzipJsonDeserializer, kafkaProperties());
        val winNotificationKafkaConsumer = new FlinkKafkaConsumer011<WinNotification>(KafkaTopics.WINS, winNotificationGzipJsonDeserializer, kafkaProperties());
        val impressionsKafkaConsumer = new FlinkKafkaConsumer011<Impression>(KafkaTopics.IMPRESSIONS, impressionGzipJsonDeserializer, kafkaProperties());
        val clicksKafkaConsumer = new FlinkKafkaConsumer011<Click>(KafkaTopics.CLICKS, clickGzipJsonDeserializer, kafkaProperties());
        val conversionsKafkaConsumer = new FlinkKafkaConsumer011<Conversion>(KafkaTopics.CONVERSIONS, conversionGzipJsonDeserializer, kafkaProperties());
        val postbacksKafkaConsumer = new FlinkKafkaConsumer011<Postback>(KafkaTopics.POSTBACKS, postbackGzipJsonDeserializer, kafkaProperties());

        // streams
        val bidReqStream = flinkEnv.addSource(bidReqKafkaConsumer);
        val bidRespStream = flinkEnv.addSource(bidRespKafkaConsumer);
        val winNotificationStream = flinkEnv.addSource(winNotificationKafkaConsumer);
        val impressionStream = flinkEnv.addSource(impressionsKafkaConsumer);
        val clickStream = flinkEnv.addSource(clicksKafkaConsumer);
        val conversionStream = flinkEnv.addSource(conversionsKafkaConsumer);
        val postbackStream = flinkEnv.addSource(postbacksKafkaConsumer);

        // windowed stream
        val bidReqWindowedStream = bidReqStream.keyBy(new AggregatedBidReqKey()).timeWindow(Time.minutes(1));
        val bidRespWindowedStream = bidRespStream.keyBy(new AggregatedBidRespKey()).timeWindow(Time.minutes(1));
        val winNotificationWindowedStream = winNotificationStream.keyBy(new AggregatedWinNotificationKey()).timeWindow(Time.minutes(1));
        val impressionWindowedStream = impressionStream.keyBy(new AggregatedImpressionReqKey()).timeWindow(Time.minutes(1));
        val clickWindowedStream = clickStream.keyBy(new AggregatedClickKey()).timeWindow(Time.minutes(1));
        val conversionWindowedStream = conversionStream.keyBy(new AggregatedConversionKey()).timeWindow(Time.minutes(1));
        val postbackWindowedStream = postbackStream.keyBy(new AggregatedPostbackKey()).timeWindow(Time.minutes(1));

        // Aggregated streams
        val aggregatedBidReqStream = bidReqWindowedStream.apply(new BidReqWindowCountFunction())
                .name("Count Bid Request in a Windowed Stream");

        val aggregatedBidRespStream = bidRespWindowedStream.apply(new BidRespWindowCountFunction())
                .name("Count Bid Response in a Windowed Stream");

        val aggregatedWinStream = winNotificationWindowedStream.apply(new WinNotificationCountFunction())
                .name("Count WinNofitication in a Windowed Stream");

        val aggregatedImpressionStream = impressionWindowedStream.apply(new ImpressionCountFunction())
                .name("Count Impression in a Windowed Stream");

        val aggregatedClickStream = clickWindowedStream.apply(new ClickCountFunction())
                .name("Count Clicks in a Windowed Stream");

        val aggregatedConversionStream = conversionWindowedStream.apply(new ConversionCountFunction())
                .name("Count Conversions in a Windowed Stream");

        val aggregatedPostbackStream = postbackWindowedStream.apply(new PostbackWindowCountFunction())
                .name("Count Postback in a Windowed Stream");

        // Serialize for Aggregated objects.
        val aggregatedBidReqJsonSerializer = new JsonSerializer<AggregatedBidReq>();
        val aggregatedBidRespJsonSerializer = new JsonSerializer<AggregatedBidResp>();
        val aggregatedWinNotificationJsonSerializer = new JsonSerializer<AggregatedWin>();
        val aggregatedImpressionJsonSerializer = new JsonSerializer<AggregatedImpression>();
        val aggregatedClickJsonSerializer = new JsonSerializer<AggregatedClick>();
        val aggregatedConversionJsonSerializer = new JsonSerializer<AggregatedConversion>();
        val aggregatedPostbackJsonSerializer = new JsonSerializer<AggregatedPostback>();

        // Sinks for aggregated objects
        val aggregatedBidReqKafkaSink = new FlinkKafkaProducer011<AggregatedBidReq>(KafkaTopics.AGGREGATED_BID_REQ, aggregatedBidReqJsonSerializer, kafkaProperties());
        val aggregatedBidRespKafkaSink = new FlinkKafkaProducer011<AggregatedBidResp>(KafkaTopics.AGGREGATED_BID_RESP, aggregatedBidRespJsonSerializer, kafkaProperties());
        val aggregatedWinKafkaSink = new FlinkKafkaProducer011<AggregatedWin>(KafkaTopics.AGGREGATED_WINS, aggregatedWinNotificationJsonSerializer, kafkaProperties());
        val aggregatedImpressionKafkaSink = new FlinkKafkaProducer011<AggregatedImpression>(KafkaTopics.AGGREGATED_IMPRESSIONS, aggregatedImpressionJsonSerializer, kafkaProperties());
        val aggregatedClickKafkaSink = new FlinkKafkaProducer011<AggregatedClick>(KafkaTopics.AGGREGATED_CLICKS, aggregatedClickJsonSerializer, kafkaProperties());
        val aggregatedConversionKafkaSink = new FlinkKafkaProducer011<AggregatedConversion>(KafkaTopics.AGGREGATED_CONVERSIONS, aggregatedConversionJsonSerializer, kafkaProperties());
        val aggregatedPostbackKafkaSink = new FlinkKafkaProducer011<AggregatedPostback>(KafkaTopics.AGGREGATED_POSTBACKS, aggregatedPostbackJsonSerializer, kafkaProperties());

        // attached sink to aggregated streams
        aggregatedBidReqStream.addSink(aggregatedBidReqKafkaSink);
        aggregatedBidRespStream.addSink(aggregatedBidRespKafkaSink);
        aggregatedWinStream.addSink(aggregatedWinKafkaSink);
        aggregatedImpressionStream.addSink(aggregatedImpressionKafkaSink);
        aggregatedClickStream.addSink(aggregatedClickKafkaSink);
        aggregatedConversionStream.addSink(aggregatedConversionKafkaSink);
        aggregatedPostbackStream.addSink(aggregatedPostbackKafkaSink);

        // execute program
        flinkEnv.execute("Count events in a time window for the mangola platform");
    }

    private static Properties kafkaProperties() {
        val properties = new Properties();

        // each key in Kafka is String
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Each value is a byte[] (Each value is a JSON string encoded as bytes)
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Zookeeper default host:port
        properties.setProperty("zookeeper.connect", "localhost:2181");

        // Broker default host:port
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("group.id", "mangola-flink-streams-processor");

        return properties;
    }
}
