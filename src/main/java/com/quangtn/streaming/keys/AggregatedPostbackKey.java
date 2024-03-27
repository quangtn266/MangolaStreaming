package com.quangtn.streaming.keys;

import com.quangtn.streaming.domain.Postback;
import com.quangtn.streaming.utils.TimeUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import lombok.val;

import java.time.ZonedDateTime;

public class AggregatedPostbackKey implements KeySelector<Postback,
        Tuple7<Integer, Integer, Integer, Integer, Integer, ZonedDateTime, String>> {

    @Override
    public Tuple7<Integer, Integer, Integer, Integer, Integer, ZonedDateTime, String> getKey(
            final Postback postback
    ) throws Exception {
        val advId = postback.getAdvId();
        val sourceId = postback.getSourceId();
        val clientId = postback.getClientId();
        val campaignId = postback.getCampaignId();
        val creativeId = postback.getCreativeId();
        val event = postback.getEvent();
        val minute  = TimeUtil.roundOffToMinute(postback.getTimestamp());

        return new Tuple7<>(advId, sourceId, clientId, campaignId, creativeId, minute, event);
    }
}
