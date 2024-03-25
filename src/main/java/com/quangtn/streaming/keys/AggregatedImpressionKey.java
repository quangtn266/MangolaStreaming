package com.quangtn.streaming.keys;

import com.quangtn.streaming.domain.Impression;
import com.quangtn.streaming.utils.TimeUtil;
import lombok.val;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.ZonedDateTime;

public class AggregatedImpressionKey implements KeySelector<Impression,
        Tuple6<Integer, Integer, Integer, Integer, Integer, ZonedDateTime>> {

    @Override
    public Tuple6<Integer, Integer, Integer, Integer, Integer, ZonedDateTime> getKey(
            final Impression impression
    ) throws Exception {
        val advId = impression.getAdvId();
        val sourceId = impression.getSourceId();
        val clientId = impression.getClientId();
        val campaignId = impression.getCampaignId();
        val creativeId = impression.getCreativeId();
        val minute = TimeUtil.roundOffToMinute(impression.getTimestamp());

        return new Tuple6<>(advId, sourceId, clientId, campaignId, creativeId, minute);
    }
}
