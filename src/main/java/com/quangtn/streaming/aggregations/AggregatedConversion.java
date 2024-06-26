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
public class AggregatedConversion {

    @JsonProperty(value = "uuid", required = true)
    @JsonSerialize(using = UUIDSerializer.class)
    private final UUID uuid;

    @JsonProperty(value = "advId", required = true)
    private final Integer advId;

    @JsonProperty(value = "clientId", required = true)
    private final Integer clientId;

    @JsonProperty(value = "sourceId", required = true)
    private final Integer sourceId;

    @JsonProperty(value = "campaignId", required = true)
    private final Integer campaignId;

    @JsonProperty(value = "creativeId", required = true)
    private final Integer creativeId;

    @JsonProperty(value = "count", required = true)
    private final Integer count;

    @JsonProperty(value = "timestamp", required = true)
    @JsonSerialize(using = ZonedDateTimeSerializer.class)
    private final ZonedDateTime timestamp;

    @JsonProperty(value = "event", required = true)
    private final String event;

    @JsonProperty(value = "eventCode", required = true)
    private final Integer eventCode;
}
