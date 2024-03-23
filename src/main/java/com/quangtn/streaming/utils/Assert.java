package com.quangtn.streaming.utils;

import java.util.Objects;

public class Assert {

    private Assert() {}

    public static <T> void notNull(final T t, final String errorMsg) {
        if(Objects.isNull(t)) {
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
