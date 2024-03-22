package com.quangtn.streaming.aggregations.functions;

import com.quangtn.streaming.aggregations.AggregatedClick;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import scala.Tuple12;

import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.stream.Collector;

public class ClickCountFunction implements WindowFunction<Click, AggregatedClick,
        Tuple12<Integer, Integer, Integer, Integer, Integer, Integer, ZonedDateTime,
                String, String, String, String, String>, TimeWindow> {

    @Override
    public void apply(
            final Tuple12<Integer, Integer, Integer, Integer, Integer, Integer, ZonedDateTime,
            String, String, String, String, String> keys,
            final TimeWindow timeWindow,
            final Iterable<Click> clicks,
            final Collector<AggregatedClick> collector
            ) throws Exception {

        val advId = (Integer) keys.getField(0);
        val sourceId = (Integer) keys.getField(1);
        val clientId = (Integer) keys.getField(2);
        val campaignId = (Integer) keys.getField(3);
        val creativeId = (Integer) keys.getField(4);
        val eventCode = (Integer) keys.getField(5);
        val minute = (ZonedDateTime) keys.getField(6);

        val city = (String) keys.getField(7);
        val country = (String) keys.getField(8);
        val province = (String) keys.getField(9);
        val carrier = (String) keys.getField(10);
        val platform = (String) keys.getField(11);

        val count = Iterables.size(clicks);
        val uuid = UUID.randomUUID();

        val aggregation = AggregatedClick.builder().uuid(uuid).advId(advId).sourceId(sourceId)
                .clientId(clientId).campaignId(campaignId).creativeId(creativeId).timestamp(minute)
                .eventCode(eventCode).city(city).country(country).province(province).carrier(carrier)
                .platform(platform).count(count).build();

        collector.collect(aggregation);
    }
}
