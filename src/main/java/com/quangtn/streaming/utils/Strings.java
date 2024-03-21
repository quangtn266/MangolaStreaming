package com.quangtn.streaming.utils;

import lombok.val;
import java.util.Objects;

public class Strings {

    public static boolean hasText(final String str) {
        if(Objects.isNull(str)) {
            return false;
        }
        for(val ch: str.toCharArray()) {
            if(!Character.isWhitespace(ch)) {
                return true;
            }
        }
        return false;
    }
}
