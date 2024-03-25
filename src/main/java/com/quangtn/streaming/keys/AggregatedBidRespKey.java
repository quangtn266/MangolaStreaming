package com.quangtn.streaming.keys;

import com.quangtn.streaming.domain.BidResp;
import com.quangtn.streaming.utils.TimeUtil;
import org.apache.flink.api.java.functions.KeySelector;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple6;

import java.time.ZonedDateTime;

public class AggregatedBidRespKey implements KeySelector<BidResp,
        Tuple6<Integer, Integer, Integer, Integer, Integer, ZonedDateTime>> {

    @Override
    public Tuple6<Integer, Integer, Integer, Integer, Integer, ZonedDateTime> getKey(
            final BidResp bidResp
    ) throws Exception {
        val advId = bidResp.getAdvId();
        val sourceId = bidResp.getSourceId();
        val clientId = bidResp.getClientId();
        val campaignId = bidResp.getCampaignId();
        val creativeId = bidResp.getCreativeId();
        val minute = TimeUtil.roundOffToMinute(bidResp.getTimestamp());

        return new Tuple6<>(advId, sourceId, clientId, campaignId, creativeId, minute);
    }
}
