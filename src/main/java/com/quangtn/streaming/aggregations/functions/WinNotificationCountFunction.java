package com.quangtn.streaming.aggregations.functions;

import com.quangtn.streaming.aggregations.AggregatedWin;
import com.quangtn.streaming.domain.WinNotification;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import lombok.val;
import java.time.ZonedDateTime;
import java.util.UUID;

public class WinNotificationCountFunction implements
        WindowFunction<WinNotification, AggregatedWin, Tuple6<Integer, Integer, Integer, Integer,
                Integer, ZonedDateTime>, TimeWindow> {

    @Override
    public void apply(
            final Tuple6<Integer, Integer, Integer, Integer, Integer, ZonedDateTime> keys,
            final TimeWindow timeWindow,
            final Iterable<WinNotification> winNotifications,
            final Collector<AggregatedWin> collector) throws Exception
    {
        val advId = (Integer) keys.getField(0);
        val sourceId = (Integer) keys.getField(1);
        val clientId = (Integer) keys.getField(2);
        val campaignId = (Integer) keys.getField(3);
        val creativeId = (Integer) keys.getField(4);
        val minute = (ZonedDateTime) keys.getField(5);

        val count = Iterables.size(winNotifications);
        val uuid = UUID.randomUUID();

        val aggregation = AggregatedWin.builder().uuid(uuid).advId(advId).sourceId(sourceId).clientId(clientId)
                .campaignId(campaignId).creativeId(creativeId).timestamp(minute).count(count).build();

        collector.collect(aggregation);
    }
}
