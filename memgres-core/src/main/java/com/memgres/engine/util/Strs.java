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

    /** What a {@code name} holds: NAMEDATALEN - 1 bytes of UTF-8. */
    public static final int NAME_BYTES = 63;

    /**
     * Cut a string down to what a {@code name} holds, which is sixty-three bytes.
     *
     * <p>The limit is in bytes, not characters, and a character is never split: a name of forty
     * two-byte characters is eighty bytes, and PostgreSQL keeps the first thirty-one of them.
     * Counted in characters instead, such a name was kept whole and was longer than any name a
     * server can hold, so what the catalogue reported about it could not have come from one.
     */
    public static String truncateName(String s) {
        if (s == null || s.length() <= NAME_BYTES / 4) return s;
        int bytes = 0;
        for (int i = 0; i < s.length(); ) {
            int cp = s.codePointAt(i);
            int width = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
            if (bytes + width > NAME_BYTES) return s.substring(0, i);
            bytes += width;
            i += Character.charCount(cp);
        }
        return s;
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
