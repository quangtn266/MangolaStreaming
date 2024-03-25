package com.quangtn.streaming.keys;

import org.apache.flink.api.java.tuple.Tuple8;
import lombok.val;
import com.quangtn.streaming.domain.Conversion;
import com.quangtn.streaming.utils.TimeUtil;
import org.apache.flink.api.java.functions.KeySelector;

import java.time.ZonedDateTime;

public class AggregatedConversionKey implements KeySelector<Conversion,
        Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, ZonedDateTime, String>> {

    @Override
    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, ZonedDateTime, String> getKey (
            final Conversion conversion
            ) throws Exception {
        val advId = conversion.getAdvId();
        val sourceId = conversion.getSourceId();
        val clientId = conversion.getClientId();
        val campaignId = conversion.getCampaignId();
        val creativeId = conversion.getCreativeId();
        val eventCode = conversion.getEventCode();
        val event = conversion.getEvent();
        val minute = TimeUtil.roundOffToMinute(conversion.getTimestamp());

        return new Tuple8<>(advId, sourceId, clientId, campaignId, creativeId, eventCode, minute, event);
    }
}
