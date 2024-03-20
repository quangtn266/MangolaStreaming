package com.quangtn.streaming;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

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

    }
}
