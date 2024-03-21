package com.quangtn.streaming.jackson.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {

    @Override
    public void serialize(final ZonedDateTime timestamp, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
        if(Objects.nonNull(timestamp)) {
            jsonGenerator.writeString(
                    DateTimeFormatter.ISO_ZONED_DATE_TIME.format(timestamp)
            );
        }
    }
}
