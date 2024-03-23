package com.quangtn.streaming.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Json {

    private Json(){}

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T toObject(final String json, final Class<T> clazz) throws Exception {
        Assert.notNull(clazz, "clazz can't be null");
        return objectMapper.readValue(json, clazz);
    }

    public static String toJson(final Object object) throws Exception {
        Assert.notNull(object, "object can't be null");
        return objectMapper.writer().writeValueAsString(object);
    }
}
