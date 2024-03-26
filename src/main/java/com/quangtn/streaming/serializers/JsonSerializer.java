package com.quangtn.streaming.serializers;

import com.quangtn.streaming.utils.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Objects;

@Slf4j
public class JsonSerializer<T> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(final T t) {
        byte[] result = new byte[0];
        if(Objects.nonNull(t)) {
            try {
                val json = Json.toJson(t);
                result = json.getBytes();
            } catch (Exception e) {
                log.error("", e);
            }
        }
        return result;
    }
}
