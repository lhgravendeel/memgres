package com.memgres.engine.util;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Java 8-compatible replacements for String methods added in Java 9-11.
 */
public final class Strs {
    private Strs() {}

    public static String repeat(String s, int count) {
        if (count < 0) throw new IllegalArgumentException("count is negative: " + count);
        if (count == 0 || s.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) sb.append(s);
        return sb.toString();
    }

    public static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String strip(String s) {
        return s == null ? null : s.trim();
    }

    public static String stripLeading(String s) {
        if (s == null) return null;
        int i = 0;
        while (i < s.length() && s.charAt(i) <= ' ') i++;
        return s.substring(i);
    }

    public static Stream<String> lines(String s) {
        return Arrays.stream(s.split("\\R", -1));
    }
}
