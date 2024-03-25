package com.quangtn.streaming.keys;

import com.quangtn.streaming.domain.BidReq;
import com.quangtn.streaming.utils.TimeUtil;
import org.apache.flink.api.java.functions.KeySelector;
import lombok.val;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.ZonedDateTime;

public class AggregatedBidReqKey implements KeySelector<BidReq,
        Tuple4<Integer, Integer, Integer, ZonedDateTime>> {

    @Override
    public Tuple4<Integer, Integer, Integer, ZonedDateTime> getKey(
            final BidReq bidReq
            ) throws Exception {
        val advId = bidReq.getAdvId();
        val sourceId = bidReq.getSourceId();
        val clientId = bidReq.getClientId();
        val minute = TimeUtil.roundOffToMinute(bidReq.getTimestamp());

        return new Tuple4<>(advId, sourceId, clientId, minute);
    }
}
