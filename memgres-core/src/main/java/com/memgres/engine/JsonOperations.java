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
        return jsonValueToText(extractPath(json, path));
    }

    /**
     * Convert a JSON value to the SQL text form returned by the ->> and #>> operators:
     * JSON null maps to SQL NULL, JSON strings are unquoted and fully unescaped,
     * and any other value is returned as its JSON text.
     */
    public static String jsonValueToText(String value) {
        if (value == null) return null;
        value = value.trim();
        if (value.equals("null")) return null;
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return unescapeJsonString(value.substring(1, value.length() - 1));
        }
        return value;
    }

    /** Unescape the contents of a JSON string literal (without the surrounding quotes). */
    public static String unescapeJsonString(String s) {
        if (s.indexOf('\\') < 0) return s;
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'n': sb.append('\n'); break;
                    case 't': sb.append('\t'); break;
                    case 'r': sb.append('\r'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'u':
                        if (i + 4 < s.length()) {
                            try {
                                sb.append((char) Integer.parseInt(s.substring(i + 1, i + 5), 16));
                                i += 4;
                            } catch (NumberFormatException e) {
                                sb.append('u');
                            }
                        } else {
                            sb.append('u');
                        }
                        break;
                    default: sb.append(n); break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Rewrite every string in a jsonb document into the form PG stores. jsonb keeps decoded
     * text rather than the escapes it was written with, so {@code "é"} and {@code "\/"}
     * come back as the characters they name. Decoding is also where the escapes json accepts
     * but jsonb cannot represent are caught: a NUL has no place in text, and a surrogate only
     * means anything as one half of a pair.
     *
     * @param json JSON text already known to be syntactically valid
     */
    public static String canonicalizeJsonbStrings(String json) {
        if (json.indexOf('\\') < 0) return json;   // nothing to decode
        StringBuilder sb = new StringBuilder(json.length());
        int i = 0;
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c != '"') { sb.append(c); i++; continue; }
            int end = findClosingQuote(json, i + 1);
            if (end < 0) { sb.append(json, i, json.length()); break; }
            appendJsonString(sb, decodeJsonString(json.substring(i + 1, end)));
            i = end + 1;
        }
        return sb.toString();
    }

    /** Decode one string body, refusing the escapes jsonb has no character for. */
    private static String decodeJsonString(String body) {
        StringBuilder out = new StringBuilder(body.length());
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c != '\\') { out.append(c); continue; }
            if (i + 1 >= body.length()) { out.append(c); break; }
            char esc = body.charAt(++i);
            switch (esc) {
                case '"': out.append('"'); break;
                case '\\': out.append('\\'); break;
                case '/': out.append('/'); break;
                case 'b': out.append('\b'); break;
                case 'f': out.append('\f'); break;
                case 'n': out.append('\n'); break;
                case 'r': out.append('\r'); break;
                case 't': out.append('\t'); break;
                case 'u': {
                    if (i + 5 > body.length()) { out.append(esc); break; }
                    int cp;
                    try {
                        cp = Integer.parseInt(body.substring(i + 1, i + 5), 16);
                    } catch (NumberFormatException e) {
                        out.append(esc);
                        break;
                    }
                    i += 4;
                    if (cp == 0) {
                        MemgresException e = new MemgresException(
                                "unsupported Unicode escape sequence", "22P05");
                        e.setDetail(String.format("\\u%04x cannot be converted to text.", cp));
                        throw e;
                    }
                    if (Character.isHighSurrogate((char) cp)) {
                        int low = readSurrogate(body, i);
                        out.append((char) cp).append((char) low);
                        i += 6;
                    } else if (Character.isLowSurrogate((char) cp)) {
                        throw invalidEscape();
                    } else {
                        out.append((char) cp);
                    }
                    break;
                }
                default: out.append(esc); break;
            }
        }
        return out.toString();
    }

    /** The second half of a surrogate pair has to follow immediately, as its own \\u escape. */
    private static int readSurrogate(String body, int i) {
        if (i + 7 > body.length() || body.charAt(i + 1) != '\\' || body.charAt(i + 2) != 'u') {
            throw invalidEscape();
        }
        int low;
        try {
            low = Integer.parseInt(body.substring(i + 3, i + 7), 16);
        } catch (NumberFormatException e) {
            throw invalidEscape();
        }
        if (!Character.isLowSurrogate((char) low)) throw invalidEscape();
        return low;
    }

    private static MemgresException invalidEscape() {
        MemgresException e = new MemgresException("invalid input syntax for type json", "22P02");
        // Every way a surrogate escape can go wrong -- a high half with nothing after it, a low
        // half on its own, a pair whose second half is not one -- is the same complaint in PG.
        e.setDetail("Unicode low surrogate must follow a high surrogate.");
        return e;
    }

    /** Render a text value as a JSON string literal, escaping what JSON cannot hold literally. */
    public static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        appendJsonString(sb, s);
        return sb.toString();
    }

    /** Write a decoded string back out, escaping only what JSON text cannot hold literally. */
    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
    }

    /** True when the JSON text is an object, i.e. the container keys can be looked up in. */
    public static boolean isObject(String json) {
        return json != null && json.trim().startsWith("{");
    }

    /** True when the JSON text is an array. */
    public static boolean isArray(String json) {
        return json != null && json.trim().startsWith("[");
    }

    /** True when the JSON text is a quoted string rather than a number, boolean or container. */
    public static boolean isString(String json) {
        if (json == null) return false;
        String t = json.trim();
        return t.length() >= 2 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"';
    }

    /**
     * True when the JSON text is a scalar — a string, number, boolean or null. PG names this
     * shape in the errors it raises for an operation that needs a container.
     */
    public static boolean isScalarValue(String json) {
        return !isObject(json) && !isArray(json);
    }

    /** True if the string is a JSON scalar literal (string, number, boolean or null). */
    public static boolean isJsonScalar(String s) {
        if (s == null) return false;
        s = s.trim();
        if (s.isEmpty() || s.startsWith("{") || s.startsWith("[")) return false;
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) return true;
        if (s.equals("true") || s.equals("false") || s.equals("null")) return true;
        try {
            new java.math.BigDecimal(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Check if left JSON contains right JSON. Implements @> (recursive). */
    public static boolean contains(String left, String right) {
        return contains(left, right, true);
    }

    private static boolean contains(String left, String right, boolean topLevel) {
        left = left.trim();
        right = right.trim();

        boolean leftIsObj = left.startsWith("{");
        boolean rightIsObj = right.startsWith("{");
        boolean leftIsArr = left.startsWith("[");
        boolean rightIsArr = right.startsWith("[");

        if (rightIsObj && leftIsObj) {
            Map<String, String> leftMap = parseObjectKeys(left);
            Map<String, String> rightMap = parseObjectKeys(right);
            for (Map.Entry<String, String> entry : rightMap.entrySet()) {
                String leftVal = leftMap.get(entry.getKey());
                if (leftVal == null) return false;
                // Recursive containment check for nested objects/arrays
                if (!contains(leftVal.trim(), entry.getValue().trim(), false)) return false;
            }
            return true;
        }
        if (rightIsArr && leftIsArr) {
            List<String> leftElems = parseArrayElements(left);
            List<String> rightElems = parseArrayElements(right);
            for (String re : rightElems) {
                boolean found = false;
                for (String le : leftElems) {
                    if (contains(le.trim(), re.trim(), false)) { found = true; break; }
                }
                if (!found) return false;
            }
            return true;
        }
        // PG special case, at the TOP level only: an array contains a scalar
        // if the scalar equals one of its elements. Nested levels do not get this.
        if (topLevel && leftIsArr && !rightIsObj) {
            for (String le : parseArrayElements(left)) {
                if (contains(le.trim(), right, false)) return true;
            }
            return false;
        }
        // Structural mismatch (object vs array, container vs scalar): not contained
        if (leftIsObj || leftIsArr || rightIsObj || rightIsArr) return false;
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
            // For arrays, ? matches top-level *string* elements only (PG semantics)
            List<String> elems = parseArrayElements(json);
            for (String e : elems) {
                String trimmed = e.trim();
                if (trimmed.startsWith("\"") && key.equals(jsonValueToText(trimmed))) return true;
            }
            return false;
        }
        // A top-level scalar string matches the key if it equals it (PG semantics)
        return json.startsWith("\"") && key.equals(jsonValueToText(json));
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

    /**
     * Delete a text key. Implements jsonb - text and jsonb - text[]: an object loses the key,
     * an array loses every top-level string element equal to it. There is nothing to delete
     * from a scalar, so PG raises rather than handing back the input.
     */
    public static String deleteKey(String json, String key) {
        json = json.trim();
        if (isObject(json)) {
            Map<String, String> map = parseObjectKeys(json);
            map.remove(key);
            return mapToJson(map);
        }
        if (isArray(json)) {
            final String literal = quote(key);
            List<String> elems = parseArrayElements(json);
            elems.removeIf(el -> el.trim().equals(literal));
            return elemsToJsonArray(elems);
        }
        throw new MemgresException("cannot delete from scalar", "22023");
    }

    /**
     * Delete an array element by position. Implements jsonb - integer. An object has no
     * positions to delete by and a scalar has nothing to delete at all.
     */
    public static String deleteIndex(String json, int idx) {
        json = json.trim();
        if (isObject(json)) {
            throw new MemgresException("cannot delete from object using integer index", "22023");
        }
        if (!isArray(json)) {
            throw new MemgresException("cannot delete from scalar", "22023");
        }
        List<String> elems = parseArrayElements(json);
        // Negative index counts from the end (PG semantics); out-of-range
        // indexes (either sign) leave the array unchanged.
        if (idx < 0) idx += elems.size();
        if (idx >= 0 && idx < elems.size()) elems.remove(idx);
        return elemsToJsonArray(elems);
    }

    /** Delete path. Implements #-. */
    public static String deletePath(String json, List<String> path) {
        if (path.isEmpty()) return json;
        json = json.trim();
        if (isScalarValue(json)) {
            throw new MemgresException("cannot delete path in scalar", "22023");
        }
        return deletePathAt(json, path, 0);
    }

    /**
     * Walk one path step. A step that runs into a scalar simply leaves the value alone — only
     * the target itself being a scalar is an error — but an array reached by a step that is not
     * an integer is a malformed path.
     */
    private static String deletePathAt(String json, List<String> path, int at) {
        json = json.trim();
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (isArray(json)) {
            List<String> elems = parseArrayElements(json);
            int idx = pathIndex(key, at);
            if (idx < 0) idx += elems.size();
            if (idx >= 0 && idx < elems.size()) {
                if (last) elems.remove(idx);
                else if (!isScalarValue(elems.get(idx))) {
                    elems.set(idx, deletePathAt(elems.get(idx), path, at + 1));
                }
            }
            return elemsToJsonArray(elems);
        }
        if (isObject(json)) {
            Map<String, String> map = parseObjectKeys(json);
            if (last) {
                map.remove(key);
                return mapToJson(map);
            }
            String child = map.get(key);
            if (child != null && !isScalarValue(child)) {
                map.put(key, deletePathAt(child, path, at + 1));
            }
            return mapToJson(map);
        }
        return json;
    }

    /** A path step addressing an array has to be an integer; PG counts the steps from one. */
    private static int pathIndex(String key, int at) {
        try {
            return Integer.parseInt(key.trim());
        } catch (NumberFormatException e) {
            throw new MemgresException(
                    "path element at position " + (at + 1) + " is not an integer: \"" + key + "\"", "22P02");
        }
    }

    /**
     * Concatenate two JSONB values. Implements || for jsonb using PG 18 semantics:
     * two objects are merged (right wins on key conflicts); in all other cases each
     * non-array input is wrapped as a single-element array and the two arrays are
     * concatenated.
     */
    public static String concatenate(String left, String right) {
        left = left.trim();
        right = right.trim();
        if (left.startsWith("{") && right.startsWith("{")) {
            Map<String, String> lMap = parseObjectKeys(left);
            Map<String, String> rMap = parseObjectKeys(right);
            lMap.putAll(rMap);
            return mapToJson(lMap);
        }
        List<String> elems = new ArrayList<>();
        if (left.startsWith("[")) elems.addAll(parseArrayElements(left));
        else elems.add(left);
        if (right.startsWith("[")) elems.addAll(parseArrayElements(right));
        else elems.add(right);
        return elemsToJsonArray(elems);
    }

    /** jsonb_set implementation with create_missing = true. */
    public static String jsonbSet(String json, List<String> path, String newValue) {
        return jsonbSet(json, path, newValue, true);
    }

    /**
     * jsonb_set implementation. PG semantics: intermediate path steps are never
     * created — if one is missing, the target is returned unchanged. The
     * create_missing flag only controls whether the FINAL path step may be added.
     * Negative array indexes count from the end.
     */
    public static String jsonbSet(String json, List<String> path, String newValue, boolean createMissing) {
        json = json.trim();
        // The answer is a jsonb whatever happens to it, so it is spelled the way jsonb is spelled
        // — including when the path reached nothing and the target comes back as it went in.
        if (path.isEmpty()) return normalizeJsonb(json);
        requirePathTarget(json);
        return normalizeJsonb(jsonbSetAt(json, path, 0, newValue, createMissing));
    }

    private static String jsonbSetAt(String json, List<String> path, int at, String newValue, boolean createMissing) {
        json = json.trim();
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (isObject(json)) {
            Map<String, String> map = parseObjectKeys(json);
            if (last) {
                if (!map.containsKey(key) && !createMissing) return json;
                map.put(key, newValue);
                return mapToJson(map);
            }
            String child = map.get(key);
            if (child == null || isScalarValue(child)) return json; // step lands nowhere: unchanged
            map.put(key, jsonbSetAt(child, path, at + 1, newValue, createMissing));
            return mapToJson(map);
        }
        if (isArray(json)) {
            int idx = pathIndex(key, at);
            List<String> elems = parseArrayElements(json);
            if (idx < 0) idx += elems.size(); // negative index counts from the end
            if (last) {
                if (idx >= 0 && idx < elems.size()) {
                    elems.set(idx, newValue);
                } else if (createMissing) {
                    // Out-of-range last step: prepend for negative, append for positive
                    if (idx < 0) elems.add(0, newValue);
                    else elems.add(newValue);
                }
                return elemsToJsonArray(elems);
            }
            if (idx >= 0 && idx < elems.size() && !isScalarValue(elems.get(idx))) {
                elems.set(idx, jsonbSetAt(elems.get(idx), path, at + 1, newValue, createMissing));
                return elemsToJsonArray(elems);
            }
            return json; // missing intermediate step: unchanged
        }
        return json;
    }

    /** A path only addresses something inside a container; a scalar has no path to set. */
    private static void requirePathTarget(String json) {
        if (isScalarValue(json)) {
            throw new MemgresException("cannot set path in scalar", "22023");
        }
    }

    /**
     * jsonb_insert implementation. PG semantics: missing intermediate path steps
     * leave the target unchanged; inserting at an existing object key is an error;
     * negative array indexes count from the end; out-of-range array positions
     * prepend (negative) or append (positive).
     */
    public static String jsonbInsert(String json, List<String> path, String newValue, boolean insertAfter) {
        if (path.isEmpty()) return json;
        json = json.trim();
        requirePathTarget(json);
        return jsonbInsertAt(json, path, 0, newValue, insertAfter);
    }

    private static String jsonbInsertAt(String json, List<String> path, int at, String newValue, boolean insertAfter) {
        json = json.trim();
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (isArray(json)) {
            int idx = pathIndex(key, at);
            List<String> elems = parseArrayElements(json);
            if (idx < 0) idx += elems.size(); // negative index counts from the end
            if (last) {
                int insertIdx = insertAfter ? idx + 1 : idx;
                if (insertIdx < 0) insertIdx = 0;                       // out-of-range negative: prepend
                if (insertIdx > elems.size()) insertIdx = elems.size(); // out-of-range positive: append
                elems.add(insertIdx, newValue);
                return elemsToJsonArray(elems);
            }
            if (idx >= 0 && idx < elems.size() && !isScalarValue(elems.get(idx))) {
                elems.set(idx, jsonbInsertAt(elems.get(idx), path, at + 1, newValue, insertAfter));
                return elemsToJsonArray(elems);
            }
            return json; // missing intermediate step: unchanged
        }
        if (isObject(json)) {
            Map<String, String> map = parseObjectKeys(json);
            if (last) {
                if (map.containsKey(key)) {
                    throw new MemgresException("cannot replace existing key\n"
                            + "  Hint: Try using the function jsonb_set to replace key value.", "22023");
                }
                map.put(key, newValue);
                return mapToJson(map);
            }
            String child = map.get(key);
            if (child == null || isScalarValue(child)) return json; // step lands nowhere: unchanged
            map.put(key, jsonbInsertAt(child, path, at + 1, newValue, insertAfter));
            return mapToJson(map);
        }
        return json;
    }

    /** Strip null values from JSON object */
    public static String stripNulls(String json) {
        return stripNulls(json, false, false);
    }

    /** Strip null values from JSON object, optionally in compact mode (no spaces). */
    public static String stripNulls(String json, boolean compact) {
        return stripNulls(json, compact, false);
    }

    /**
     * The same, with the second argument PostgreSQL's strip_nulls takes. A null member of an
     * object always goes; {@code stripInArrays} says whether a null element of an array goes with
     * it, which is not the default because dropping one moves every element after it.
     */
    public static String stripNulls(String json, boolean compact, boolean stripInArrays) {
        json = json.trim();
        if (json.startsWith("{")) {
            Map<String, String> map = parseObjectKeys(json);
            map.entrySet().removeIf(e -> "null".equals(e.getValue().trim()));
            for (Map.Entry<String, String> entry : map.entrySet()) {
                entry.setValue(stripNulls(entry.getValue(), compact, stripInArrays));
            }
            return mapToJson(map, compact);
        }
        if (json.startsWith("[")) {
            List<String> kept = new ArrayList<>();
            for (String elem : parseArrayElements(json)) {
                if (stripInArrays && "null".equals(elem.trim())) continue;
                kept.add(stripNulls(elem, compact, stripInArrays));
            }
            return elemsToJsonArray(kept, compact);
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
            // Inside a string a backslash escapes the next character, so an escaped quote or
            // backslash cannot end the string
            if (inString && c == '\\' && i + 1 < json.length()) {
                sb.append(c).append(json.charAt(++i));
            } else if (c == '"') {
                inString = !inString;
                sb.append(c);
            } else if (!inString) {
                if (c == '{' || c == '[') {
                    sb.append(c);
                    indent += 4;
                    // A container holding nothing puts its closing brace on the next line with
                    // nothing indented in between, so the indented line every other container
                    // opens with is not written for it.
                    if (!isEmptyContainer(json, i)) {
                        sb.append('\n').append(Strs.repeat(" ", indent));
                    }
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

    /** True when the container opening at {@code open} holds nothing but whitespace. */
    private static boolean isEmptyContainer(String json, int open) {
        char close = json.charAt(open) == '{' ? '}' : ']';
        for (int i = open + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == ' ' || c == '\n' || c == '\r' || c == '\t') continue;
            return c == close;
        }
        return false;
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
        return parseObject(json, new TreeMap<String, String>((a, b) -> {
            // PG jsonb key ordering: shorter keys first, then lexicographic
            if (a.length() != b.length()) return Integer.compare(a.length(), b.length());
            return a.compareTo(b);
        }));
    }

    /**
     * The members of an object in the order the text lists them. jsonb reorders its keys when it
     * stores them, but the json type keeps the document as written, so anything that walks a json
     * value has to walk it in that order.
     */
    public static Map<String, String> parseObjectKeysInOrder(String json) {
        return parseObject(json, new java.util.LinkedHashMap<String, String>());
    }

    private static Map<String, String> parseObject(String json, Map<String, String> result) {
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
            // Keys are held decoded, the way a text key given to -> or jsonb_set arrives, and
            // are escaped again on the way out
            String key = unescapeJsonString(inner.substring(keyStart, keyEnd));
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
            // Safety: guarantee forward progress. If findValueEnd made no progress
            // (e.g., the current char is a stray '}' or ']' from malformed input),
            // advance past it so we cannot spin forever and OOM via unbounded add().
            if (i == valStart) i++;
        }
        return result;
    }

    private static int findClosingQuote(String s, int start) {
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            // A backslash always escapes the next character, so "a\\" ends at the quote after
            // the pair; looking only at the previous character mistook that quote for an escape.
            if (c == '\\') { i++; continue; }
            if (c == '"') return i;
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
                if (inStr && c == '\\') { i++; continue; }  // an escaped quote does not end the string
                if (c == '"') { inStr = !inStr; continue; }
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

    private static String mapToJson(Map<String, String> map, boolean compact) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        String sep = compact ? "," : ", ";
        String colon = compact ? ":" : ": ";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) sb.append(sep);
            appendJsonString(sb, entry.getKey());
            sb.append(colon).append(doNormalize(entry.getValue(), compact));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String mapToJson(Map<String, String> map) {
        return mapToJson(map, false);
    }

    /**
     * Recursively normalize a JSONB value with spaces (PG jsonb text output style).
     */
    public static String normalizeJsonb(String json) {
        return doNormalize(json, false);
    }

    /**
     * Recursively normalize a JSONB value in compact form (no spaces), used by JSON_QUERY.
     */
    public static String normalizeJsonbCompact(String json) {
        return doNormalize(json, true);
    }

    private static String doNormalize(String json, boolean compact) {
        if (json == null) return "null";
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            Map<String, String> map = parseObjectKeys(json);
            return mapToJson(map, compact);
        }
        if (json.startsWith("[") && json.endsWith("]")) {
            List<String> elems = parseArrayElements(json);
            StringBuilder sb = new StringBuilder("[");
            String sep = compact ? "," : ", ";
            for (int i = 0; i < elems.size(); i++) {
                if (i > 0) sb.append(sep);
                sb.append(doNormalize(elems.get(i), compact));
            }
            sb.append("]");
            return sb.toString();
        }
        return normalizeJsonbNumber(json);
    }

    /**
     * A jsonb number is stored as {@code numeric}, so the exponent it was written with is not part
     * of it: 1e2 reads back as 100 and 1e-3 as 0.001. {@code json} keeps the text as written,
     * which is why this belongs to the jsonb path alone.
     */
    private static String normalizeJsonbNumber(String scalar) {
        String s = scalar.trim();
        if (s.isEmpty()) return scalar;
        int e = s.indexOf('e');
        if (e < 0) e = s.indexOf('E');
        if (e < 0) return scalar;
        // Only a bare number carries an exponent; a quoted string keeps whatever it holds.
        if (s.charAt(0) == '"') return scalar;
        try {
            return new java.math.BigDecimal(s).toPlainString();
        } catch (NumberFormatException ex) {
            return scalar;
        }
    }

    private static String elemsToJsonArray(List<String> elems) {
        return elemsToJsonArray(elems, false);
    }

    /** The same, written the compact way the json type's own text output is written. */
    private static String elemsToJsonArray(List<String> elems, boolean compact) {
        StringBuilder sb = new StringBuilder("[");
        String sep = compact ? "," : ", ";
        for (int i = 0; i < elems.size(); i++) {
            if (i > 0) sb.append(sep);
            sb.append(elems.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}
