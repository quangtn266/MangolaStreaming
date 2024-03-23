package com.quangtn.streaming.utils;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeUtil {

    public static ZonedDateTime roundOffToMinute(ZonedDateTime timestamp) {
        Assert.notNull(timestamp, "timestamp can't be null");
        return ZonedDateTime.of(timestamp.toLocalDateTime(), ZoneId("UTC"));
    }
}
