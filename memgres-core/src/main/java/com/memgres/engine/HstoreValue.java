package com.memgres.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a PostgreSQL hstore value — a set of key/value string pairs.
 * Literal format: {@code "key1"=>"value1", "key2"=>"value2"}
 */
public final class HstoreValue {

    private final Map<String, String> data;

    public HstoreValue(Map<String, String> data) {
        this.data = new LinkedHashMap<>(data);
    }

    /** Parse an hstore literal such as {@code a=>1, b=>2} or {@code "a"=>"1","b"=>"2"}. */
    public static HstoreValue parse(String text) {
        Map<String, String> map = new LinkedHashMap<>();
        if (text == null || text.trim().isEmpty()) return new HstoreValue(map);

        String s = text.trim();
        int i = 0;
        int len = s.length();

        while (i < len) {
            // skip whitespace and commas
            while (i < len && (s.charAt(i) == ' ' || s.charAt(i) == ',' || s.charAt(i) == '\t')) i++;
            if (i >= len) break;

            // parse key
            String key = parseToken(s, i);
            i += rawTokenLength(s, i);

            // skip whitespace
            while (i < len && s.charAt(i) == ' ') i++;

            // expect =>
            if (i < len && s.charAt(i) == '=' && i + 1 < len && s.charAt(i + 1) == '>') {
                i += 2;
            }

            // skip whitespace
            while (i < len && s.charAt(i) == ' ') i++;

            // parse value
            if (i >= len) {
                map.put(key, null);
                break;
            }
            String val;
            // check for NULL (unquoted)
            if (i + 4 <= len && s.substring(i, i + 4).equalsIgnoreCase("NULL")
                    && (i + 4 >= len || s.charAt(i + 4) == ',' || s.charAt(i + 4) == ' ')) {
                val = null;
                i += 4;
            } else {
                val = parseToken(s, i);
                i += rawTokenLength(s, i);
            }
            map.put(key, val);
        }
        return new HstoreValue(map);
    }

    private static String parseToken(String s, int start) {
        if (start >= s.length()) return "";
        if (s.charAt(start) == '"') {
            // quoted token
            StringBuilder sb = new StringBuilder();
            int i = start + 1;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '\\' && i + 1 < s.length()) {
                    sb.append(s.charAt(i + 1));
                    i += 2;
                } else if (c == '"') {
                    break;
                } else {
                    sb.append(c);
                    i++;
                }
            }
            return sb.toString();
        } else {
            // unquoted token — ends at =>, comma, or whitespace
            int i = start;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == ',' || c == ' ' || c == '\t') break;
                if (c == '=' && i + 1 < s.length() && s.charAt(i + 1) == '>') break;
                i++;
            }
            return s.substring(start, i);
        }
    }

    /** Returns how many raw characters a token (including quotes) occupies. */
    private static int rawTokenLength(String s, int start) {
        if (start >= s.length()) return 0;
        if (s.charAt(start) == '"') {
            int i = start + 1;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '\\' && i + 1 < s.length()) {
                    i += 2;
                } else if (c == '"') {
                    return i - start + 1;
                } else {
                    i++;
                }
            }
            return i - start;
        } else {
            int i = start;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == ',' || c == ' ' || c == '\t') break;
                if (c == '=' && i + 1 < s.length() && s.charAt(i + 1) == '>') break;
                i++;
            }
            return i - start;
        }
    }

    /** Get value by key, or null if not present. */
    public String get(String key) {
        return data.get(key);
    }

    /** Check if this hstore contains the given key. */
    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    /** Check if this hstore contains all key-value pairs from another hstore. */
    public boolean containsAll(HstoreValue other) {
        for (Map.Entry<String, String> entry : other.data.entrySet()) {
            String val = data.get(entry.getKey());
            if (val == null && !data.containsKey(entry.getKey())) return false;
            if (entry.getValue() == null) {
                if (val != null) return false;
            } else {
                if (!entry.getValue().equals(val)) return false;
            }
        }
        return true;
    }

    public Map<String, String> getData() {
        return Collections.unmodifiableMap(data);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append('"').append(entry.getKey()).append('"');
            sb.append("=>");
            if (entry.getValue() == null) {
                sb.append("NULL");
            } else {
                sb.append('"').append(entry.getValue()).append('"');
            }
        }
        return sb.toString();
    }
}
