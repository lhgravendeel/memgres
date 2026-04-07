package com.memgres.engine;

import com.memgres.engine.util.Strs;

import java.util.*;

/**
 * JSON/JSONB operations helper. Implements JSON manipulation without external JSON libraries.
 */
public final class JsonOperations {

    private JsonOperations() {}

    /** Extract value by path (array of keys). Implements #>. */
    public static String extractPath(String json, List<String> path) {
        String current = json;
        for (String key : path) {
            if (current == null) return null;
            current = current.trim();
            try {
                int idx = Integer.parseInt(key);
                current = extractArrayElement(current, idx);
            } catch (NumberFormatException e) {
                current = extractKey(current, key);
            }
        }
        return current;
    }

    /** Extract text value by path. Implements #>>. */
    public static String extractPathText(String json, List<String> path) {
        String result = extractPath(json, path);
        if (result == null) return null;
        result = result.trim();
        if (result.startsWith("\"") && result.endsWith("\"")) {
            return result.substring(1, result.length() - 1);
        }
        return result;
    }

    /** Check if left JSON contains right JSON. Implements @> (recursive). */
    public static boolean contains(String left, String right) {
        left = left.trim();
        right = right.trim();

        if (right.startsWith("{") && left.startsWith("{")) {
            Map<String, String> leftMap = parseObjectKeys(left);
            Map<String, String> rightMap = parseObjectKeys(right);
            for (Map.Entry<String, String> entry : rightMap.entrySet()) {
                String leftVal = leftMap.get(entry.getKey());
                if (leftVal == null) return false;
                // Recursive containment check for nested objects/arrays
                if (!contains(leftVal.trim(), entry.getValue().trim())) return false;
            }
            return true;
        }
        if (right.startsWith("[") && left.startsWith("[")) {
            List<String> leftElems = parseArrayElements(left);
            List<String> rightElems = parseArrayElements(right);
            for (String re : rightElems) {
                boolean found = false;
                for (String le : leftElems) {
                    if (contains(le.trim(), re.trim())) { found = true; break; }
                }
                if (!found) return false;
            }
            return true;
        }
        return left.equals(right);
    }

    /** Check if key exists in JSON object. Implements ?. */
    public static boolean keyExists(String json, String key) {
        json = json.trim();
        if (json.startsWith("{")) {
            Map<String, String> map = parseObjectKeys(json);
            return map.containsKey(key);
        }
        if (json.startsWith("[")) {
            List<String> elems = parseArrayElements(json);
            for (String e : elems) {
                String trimmed = e.trim();
                if (trimmed.equals("\"" + key + "\"") || trimmed.equals(key)) return true;
            }
        }
        return false;
    }

    /** Check if any key exists. Implements ?|. */
    public static boolean anyKeyExists(String json, List<String> keys) {
        for (String key : keys) {
            if (keyExists(json, key)) return true;
        }
        return false;
    }

    /** Check if all keys exist. Implements ?&amp;. */
    public static boolean allKeysExist(String json, List<String> keys) {
        for (String key : keys) {
            if (!keyExists(json, key)) return false;
        }
        return true;
    }

    /** Delete key from object or index from array. Implements jsonb - text/int. */
    public static String deleteKey(String json, String key) {
        json = json.trim();
        if (json.startsWith("{")) {
            Map<String, String> map = parseObjectKeys(json);
            map.remove(key);
            return mapToJson(map);
        }
        if (json.startsWith("[")) {
            try {
                int idx = Integer.parseInt(key);
                List<String> elems = parseArrayElements(json);
                if (idx >= 0 && idx < elems.size()) elems.remove(idx);
                return elemsToJsonArray(elems);
            } catch (NumberFormatException e) {
                List<String> elems = parseArrayElements(json);
                elems.removeIf(el -> el.trim().equals("\"" + key + "\""));
                return elemsToJsonArray(elems);
            }
        }
        return json;
    }

    /** Delete path. Implements #-. */
    public static String deletePath(String json, List<String> path) {
        if (path.isEmpty()) return json;
        if (path.size() == 1) return deleteKey(json, path.get(0));
        String parentKey = path.get(0);
        json = json.trim();
        if (json.startsWith("{")) {
            Map<String, String> map = parseObjectKeys(json);
            String child = map.get(parentKey);
            if (child != null) {
                map.put(parentKey, deletePath(child, path.subList(1, path.size())));
            }
            return mapToJson(map);
        }
        return json;
    }

    /** Concatenate two JSON values. Implements || for jsonb. */
    public static String concatenate(String left, String right) {
        left = left.trim();
        right = right.trim();
        if (left.startsWith("{") && right.startsWith("{")) {
            Map<String, String> lMap = parseObjectKeys(left);
            Map<String, String> rMap = parseObjectKeys(right);
            lMap.putAll(rMap);
            return mapToJson(lMap);
        }
        if (left.startsWith("[") && right.startsWith("[")) {
            List<String> lElems = parseArrayElements(left);
            List<String> rElems = parseArrayElements(right);
            lElems.addAll(rElems);
            return elemsToJsonArray(lElems);
        }
        if (left.startsWith("[")) {
            List<String> lElems = parseArrayElements(left);
            lElems.add(right);
            return elemsToJsonArray(lElems);
        }
        if (right.startsWith("[")) {
            List<String> rElems = parseArrayElements(right);
            rElems.add(0, left);
            return elemsToJsonArray(rElems);
        }
        return left;
    }

    /** jsonb_set implementation */
    public static String jsonbSet(String json, List<String> path, String newValue) {
        if (path.isEmpty()) return newValue;
        json = json.trim();
        String key = path.get(0);
        if (path.size() == 1) {
            if (json.startsWith("{")) {
                Map<String, String> map = parseObjectKeys(json);
                map.put(key, newValue);
                return mapToJson(map);
            }
            if (json.startsWith("[")) {
                try {
                    int idx = Integer.parseInt(key);
                    List<String> elems = parseArrayElements(json);
                    if (idx >= 0 && idx < elems.size()) elems.set(idx, newValue);
                    return elemsToJsonArray(elems);
                } catch (NumberFormatException e) { return json; }
            }
        } else {
            if (json.startsWith("{")) {
                Map<String, String> map = parseObjectKeys(json);
                String child = map.getOrDefault(key, "{}");
                map.put(key, jsonbSet(child, path.subList(1, path.size()), newValue));
                return mapToJson(map);
            }
        }
        return json;
    }

    /** jsonb_insert implementation */
    public static String jsonbInsert(String json, List<String> path, String newValue, boolean insertAfter) {
        if (path.isEmpty()) return json;
        json = json.trim();
        if (path.size() == 1 && json.startsWith("[")) {
            try {
                int idx = Integer.parseInt(path.get(0));
                List<String> elems = parseArrayElements(json);
                int insertIdx = insertAfter ? idx + 1 : idx;
                if (insertIdx >= 0 && insertIdx <= elems.size()) elems.add(insertIdx, newValue);
                return elemsToJsonArray(elems);
            } catch (NumberFormatException e) { /* fall through */ }
        }
        // Navigate into nested object/array and insert at the final level
        if (path.size() > 1) {
            String key = path.get(0);
            if (json.startsWith("{")) {
                Map<String, String> map = parseObjectKeys(json);
                String child = map.getOrDefault(key, "{}");
                map.put(key, jsonbInsert(child, path.subList(1, path.size()), newValue, insertAfter));
                return mapToJson(map);
            }
            if (json.startsWith("[")) {
                try {
                    int idx = Integer.parseInt(key);
                    List<String> elems = parseArrayElements(json);
                    if (idx >= 0 && idx < elems.size()) {
                        elems.set(idx, jsonbInsert(elems.get(idx), path.subList(1, path.size()), newValue, insertAfter));
                    }
                    return elemsToJsonArray(elems);
                } catch (NumberFormatException e) { /* fall through */ }
            }
        }
        return jsonbSet(json, path, newValue);
    }

    /** Strip null values from JSON object */
    public static String stripNulls(String json) {
        json = json.trim();
        if (json.startsWith("{")) {
            Map<String, String> map = parseObjectKeys(json);
            map.entrySet().removeIf(e -> "null".equals(e.getValue().trim()));
            for (Map.Entry<String, String> entry : map.entrySet()) {
                entry.setValue(stripNulls(entry.getValue()));
            }
            return mapToJson(map);
        }
        if (json.startsWith("[")) {
            List<String> elems = parseArrayElements(json);
            for (int i = 0; i < elems.size(); i++) {
                elems.set(i, stripNulls(elems.get(i)));
            }
            return elemsToJsonArray(elems);
        }
        return json;
    }

    /** Pretty-print JSON with indentation */
    public static String pretty(String json) {
        if (json == null) return null;
        json = json.trim();
        StringBuilder sb = new StringBuilder();
        int indent = 0;
        boolean inString = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inString = !inString;
                sb.append(c);
            } else if (!inString) {
                if (c == '{' || c == '[') {
                    sb.append(c);
                    indent += 4;
                    sb.append('\n').append(Strs.repeat(" ", indent));
                } else if (c == '}' || c == ']') {
                    indent -= 4;
                    sb.append('\n').append(Strs.repeat(" ", Math.max(0, indent))).append(c);
                } else if (c == ',') {
                    sb.append(c).append('\n').append(Strs.repeat(" ", indent));
                } else if (c == ':') {
                    sb.append(": ");
                } else if (c != ' ' && c != '\n' && c != '\r' && c != '\t') {
                    sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ---- JSON array element access ----

    public static String extractArrayElement(String json, int index) {
        json = json.trim();
        if (!json.startsWith("[")) return null;
        List<String> elems = parseArrayElements(json);
        if (index < 0) index = elems.size() + index;
        if (index >= 0 && index < elems.size()) return elems.get(index).trim();
        return null;
    }

    // ---- Key extraction ----

    public static String extractKey(String json, String key) {
        json = json.trim();
        if (!json.startsWith("{")) return null;
        Map<String, String> map = parseObjectKeys(json);
        return map.get(key);
    }

    // ---- JSON parsing helpers ----

    public static Map<String, String> parseObjectKeys(String json) {
        Map<String, String> result = new TreeMap<>((a, b) -> {
            // PG jsonb key ordering: shorter keys first, then lexicographic
            if (a.length() != b.length()) return Integer.compare(a.length(), b.length());
            return a.compareTo(b);
        });
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return result;
        String inner = json.substring(1, json.length() - 1).trim();
        if (inner.isEmpty()) return result;

        int i = 0;
        while (i < inner.length()) {
            while (i < inner.length() && Character.isWhitespace(inner.charAt(i))) i++;
            if (i >= inner.length()) break;

            if (inner.charAt(i) != '"') break;
            int keyStart = i + 1;
            int keyEnd = findClosingQuote(inner, keyStart);
            if (keyEnd < 0) break;
            String key = inner.substring(keyStart, keyEnd);
            i = keyEnd + 1;

            while (i < inner.length() && (inner.charAt(i) == ':' || Character.isWhitespace(inner.charAt(i)))) i++;

            int valStart = i;
            int valEnd = findValueEnd(inner, valStart);
            String value = inner.substring(valStart, valEnd).trim();
            result.put(key, value);
            i = valEnd;

            while (i < inner.length() && (inner.charAt(i) == ',' || Character.isWhitespace(inner.charAt(i)))) i++;
        }
        return result;
    }

    public static List<String> parseArrayElements(String json) {
        List<String> result = new ArrayList<>();
        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]")) return result;
        String inner = json.substring(1, json.length() - 1).trim();
        if (inner.isEmpty()) return result;

        int i = 0;
        while (i < inner.length()) {
            while (i < inner.length() && Character.isWhitespace(inner.charAt(i))) i++;
            if (i >= inner.length()) break;
            int valStart = i;
            int valEnd = findValueEnd(inner, valStart);
            result.add(inner.substring(valStart, valEnd).trim());
            i = valEnd;
            while (i < inner.length() && (inner.charAt(i) == ',' || Character.isWhitespace(inner.charAt(i)))) i++;
        }
        return result;
    }

    private static int findClosingQuote(String s, int start) {
        for (int i = start; i < s.length(); i++) {
            if (s.charAt(i) == '"' && (i == start || s.charAt(i - 1) != '\\')) return i;
        }
        return -1;
    }

    private static int findValueEnd(String s, int start) {
        if (start >= s.length()) return start;
        char first = s.charAt(start);
        if (first == '"') {
            int end = findClosingQuote(s, start + 1);
            return end >= 0 ? end + 1 : s.length();
        }
        if (first == '{' || first == '[') {
            int depth = 0;
            boolean inStr = false;
            for (int i = start; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '"' && (i == start || s.charAt(i - 1) != '\\')) inStr = !inStr;
                if (!inStr) {
                    if (c == '{' || c == '[') depth++;
                    else if (c == '}' || c == ']') { depth--; if (depth == 0) return i + 1; }
                }
            }
            return s.length();
        }
        int i = start;
        while (i < s.length() && s.charAt(i) != ',' && s.charAt(i) != '}' && s.charAt(i) != ']') i++;
        return i;
    }

    private static String mapToJson(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) sb.append(", ");
            sb.append("\"").append(entry.getKey()).append("\": ").append(normalizeJsonb(entry.getValue()));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Recursively normalize a JSONB value, re-ordering object keys by PG's length-first rule.
     */
    public static String normalizeJsonb(String json) {
        if (json == null) return "null";
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            Map<String, String> map = parseObjectKeys(json);
            return mapToJson(map);
        }
        if (json.startsWith("[") && json.endsWith("]")) {
            List<String> elems = parseArrayElements(json);
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < elems.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(normalizeJsonb(elems.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
        return json;
    }

    private static String elemsToJsonArray(List<String> elems) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < elems.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(elems.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
