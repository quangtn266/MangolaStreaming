package com.quangtn.streaming.jackson.deserializers;

import com.quangtn.streaming.utils.Strings;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import lombok.val;

public class ZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime> {

    @Override
    public ZonedDateTime deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        val text = jsonParser.getText();
        ZonedDateTime result = null;
        if(Strings.hasText(text)) {
            result = ZonedDateTime.parse(
                    text, DateTimeFormatter.ISO_ZONED_DATE_TIME
            );
        }
        return result;
    }
}
