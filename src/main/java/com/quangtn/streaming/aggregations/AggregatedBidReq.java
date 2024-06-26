package com.quangtn.streaming.aggregations;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.quangtn.streaming.jackson.serializers.UUIDSerializer;
import com.quangtn.streaming.jackson.serializers.ZonedDateTimeSerializer;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@Builder
public class AggregatedBidReq {

    @JsonProperty(value = "uuid", required = true)
    @JsonSerialize(using = UUIDSerializer.class)
    private final UUID uuid;

    @JsonProperty(value = "advId", required = true)
    private final Integer advId;

    @JsonProperty(value = "clientId", required = true)
    private final Integer clientId;

    @JsonProperty(value = "sourceId", required = true)
    private final Integer sourceId;

    @JsonProperty(value = "count", required = true)
    private final Integer count;

    @JsonProperty(value = "timestamp", required = true)
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    private final ZonedDateTime timestamp;
}
