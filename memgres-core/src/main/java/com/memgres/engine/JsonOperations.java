package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The json and jsonb operators.
 *
 * <p>Every one of these used to be a piece of string handling. A value was found by counting
 * brackets, two values were the same if their texts matched, and a key was written out by putting
 * quotes around it. Each of those is right for the documents people write by hand and wrong for the
 * rest: a {@code }} inside a string ended a value early, {@code 1} and {@code 1.0} were two
 * different jsonb values, and a key holding a quote produced a document that could not be read
 * back.
 *
 * <p>The operators are still text in and text out, because that is how the engine carries a json
 * value, but the work in between is now done on parsed documents. Two rules decide which of the two
 * readings each operator gets:
 *
 * <ul>
 *   <li>An operator that hands back part of its input — {@code ->}, {@code #>} and the rest of the
 *       extraction family — keeps the text that part was written with. json is not normalised on
 *       the way through, so a json member spelled {@code 1e2} comes back spelled {@code 1e2}.
 *   <li>An operator that builds a new document — {@code ||}, {@code -}, {@code jsonb_set} — is a
 *       jsonb operator, and writes what jsonb writes.
 * </ul>
 */
public final class JsonOperations {

    private JsonOperations() {}

    // ---- reading and writing ----

    /** A document as jsonb stores it. */
    static JsonValue jsonbValue(String json) {
        return JsonParser.parseJsonb(json);
    }

    /** A document as it was written. */
    static JsonValue jsonValue(String json) {
        return JsonParser.parse(json);
    }

    /** The members of a container, each with the text its value was written with. */
    static List<JsonParser.Member> members(String json) {
        List<JsonParser.Member> members = JsonParser.membersOf(json);
        return members == null ? new ArrayList<JsonParser.Member>() : members;
    }

    // ---- paths ----

    /**
     * The path a text array argument names, read as the array literal it is.
     *
     * <p>This used to be a split on commas, which is right only for the paths that hold neither a
     * comma nor a quote: a quoted key kept its quotes, a key holding a comma was torn in two, and
     * the empty array came out as a path of one empty key rather than the empty path that names
     * the whole document. A null element is kept as one, because it is not a key and the
     * operations differ over what to do about it.
     */
    public static List<String> parsePathArray(Object arg) {
        List<?> elements;
        if (arg instanceof List<?>) {
            elements = (List<?>) arg;
        } else {
            String s = arg.toString().trim();
            // Anything not written as an array is a path of one element; the callers that refuse
            // such an argument outright have already done so.
            if (!s.startsWith("{")) return Cols.listOf(s);
            elements = FunctionEvaluator.parseSimplePgArray(s);
        }
        List<String> path = new ArrayList<>();
        for (Object element : elements) path.add(element == null ? null : element.toString());
        return path;
    }

    /**
     * Refuses a path with a null element, which the operations that build a new document do: there
     * is no member to put the value under. The operations that only read one answer NULL instead.
     */
    public static void requireNoNullPathElement(List<String> path) {
        for (int i = 0; i < path.size(); i++) {
            if (path.get(i) == null) {
                throw new MemgresException(
                        "path element at position " + (i + 1) + " is null", "22004");
            }
        }
    }

    // ---- extraction ----

    /**
     * The value a path names, as the document wrote it. Implements {@code #>}.
     *
     * <p>Which kind of step a path element is depends on the container it is applied to, not on
     * what the element looks like: an object is looked up by key even when the key is spelled with
     * digits, which is the only way {@code '{"1": "x"}' #> '{1}'} can find anything.
     */
    public static String extractPath(String json, List<String> path) {
        String current = json;
        for (String step : path) {
            // A null element names nothing, so the path as a whole reaches nothing.
            if (current == null || step == null) return null;
            current = extractStep(current, step);
        }
        return current;
    }

    private static String extractStep(String json, String step) {
        String t = json.trim();
        if (t.startsWith("{")) return extractKey(t, step);
        if (t.startsWith("[")) {
            int idx;
            try {
                idx = Integer.parseInt(step.trim());
            } catch (NumberFormatException e) {
                return null;   // an array has no key to look up, so the path reaches nothing
            }
            return extractArrayElement(t, idx);
        }
        return null;
    }

    /** The text form of the value a path names. Implements {@code #>>}. */
    public static String extractPathText(String json, List<String> path) {
        return jsonValueToText(extractPath(json, path));
    }

    /**
     * A JSON value as the text form {@code ->>} and {@code #>>} return: JSON null becomes SQL NULL,
     * a string gives up its quotes and its escapes, and anything else is its own text.
     *
     * <p>Decoding the string is where an escape that names no character is refused, which is what
     * makes {@code ('{"a": "\u005cu0000"}'::json) ->> 'a'} an error rather than a NUL handed to a
     * client that has nowhere to put one.
     */
    public static String jsonValueToText(String value) {
        if (value == null) return null;
        String t = value.trim();
        if (t.equals("null")) return null;
        if (t.startsWith("\"")) return JsonParser.parse(t).asString();
        return t;
    }

    /** A text value as a JSON string literal, escaping what JSON cannot hold literally. */
    public static String quote(String s) {
        return JsonWriter.quote(s);
    }

    // ---- shape tests ----

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
        String t = s.trim();
        if (t.isEmpty() || t.startsWith("{") || t.startsWith("[")) return false;
        try {
            return JsonParser.parse(t).isScalar();
        } catch (RuntimeException e) {
            return false;
        }
    }

    // ---- containment and key tests ----

    /** True when the left document contains the right one. Implements {@code @>}. */
    public static boolean contains(String left, String right) {
        return contains(jsonbValue(left), jsonbValue(right), true);
    }

    /**
     * PostgreSQL's containment: the two structures have to match, member for member, except that a
     * top-level array counts as containing a scalar equal to one of its elements. That exception is
     * the top level's alone — {@code '{"a": [1]}' @> '{"a": 1}'} is false.
     */
    private static boolean contains(JsonValue left, JsonValue right, boolean topLevel) {
        if (right.isObject() && left.isObject()) {
            for (int i = 0; i < right.size(); i++) {
                JsonValue mine = left.member(right.keyAt(i));
                if (mine == null || !contains(mine, right.at(i), false)) return false;
            }
            return true;
        }
        if (right.isArray() && left.isArray()) {
            for (int i = 0; i < right.size(); i++) {
                boolean found = false;
                for (int j = 0; j < left.size(); j++) {
                    if (contains(left.at(j), right.at(i), false)) { found = true; break; }
                }
                if (!found) return false;
            }
            return true;
        }
        if (topLevel && left.isArray() && !right.isObject()) {
            for (int j = 0; j < left.size(); j++) {
                if (contains(left.at(j), right, false)) return true;
            }
            return false;
        }
        if (left.isObject() || left.isArray() || right.isObject() || right.isArray()) return false;
        return JsonValue.compare(left, right) == 0;
    }

    /** True when the document holds the key. Implements {@code ?}. */
    public static boolean keyExists(String json, String key) {
        JsonValue value = jsonbValue(json);
        if (value.isObject()) return value.hasMember(key);
        if (value.isArray()) {
            // An array is searched for a string element equal to the key, not for a position
            for (int i = 0; i < value.size(); i++) {
                JsonValue element = value.at(i);
                if (element.kind() == JsonValue.STRING && key.equals(element.asString())) return true;
            }
            return false;
        }
        return value.kind() == JsonValue.STRING && key.equals(value.asString());
    }

    /** True when the document holds any of the keys. Implements {@code ?|}. */
    public static boolean anyKeyExists(String json, List<String> keys) {
        for (String key : keys) {
            if (keyExists(json, key)) return true;
        }
        return false;
    }

    /** True when the document holds every one of the keys. Implements {@code ?&amp;}. */
    public static boolean allKeysExist(String json, List<String> keys) {
        for (String key : keys) {
            if (!keyExists(json, key)) return false;
        }
        return true;
    }

    // ---- building new documents ----

    /**
     * Delete a text key. Implements {@code jsonb - text} and {@code jsonb - text[]}: an object
     * loses the key, an array loses every top-level string element equal to it. There is nothing to
     * delete from a scalar, so PG raises rather than handing back the input.
     */
    public static String deleteKey(String json, String key) {
        JsonValue value = jsonbValue(json);
        if (value.isObject()) {
            List<String> keys = new ArrayList<String>();
            List<JsonValue> values = new ArrayList<JsonValue>();
            for (int i = 0; i < value.size(); i++) {
                if (value.keyAt(i).equals(key)) continue;
                keys.add(value.keyAt(i));
                values.add(value.at(i));
            }
            return JsonWriter.jsonb(JsonValue.object(keys, values));
        }
        if (value.isArray()) {
            List<JsonValue> kept = new ArrayList<JsonValue>();
            for (int i = 0; i < value.size(); i++) {
                JsonValue element = value.at(i);
                if (element.kind() == JsonValue.STRING && key.equals(element.asString())) continue;
                kept.add(element);
            }
            return JsonWriter.jsonb(JsonValue.array(kept));
        }
        throw new MemgresException("cannot delete from scalar", "22023");
    }

    /**
     * Delete an array element by position. Implements {@code jsonb - integer}. An object has no
     * positions to delete by and a scalar has nothing to delete at all.
     */
    public static String deleteIndex(String json, int idx) {
        JsonValue value = jsonbValue(json);
        if (value.isObject()) {
            throw new MemgresException("cannot delete from object using integer index", "22023");
        }
        if (!value.isArray()) {
            throw new MemgresException("cannot delete from scalar", "22023");
        }
        List<JsonValue> elements = value.copyElements();
        // A negative position counts from the end; a position off either end changes nothing
        if (idx < 0) idx += elements.size();
        if (idx >= 0 && idx < elements.size()) elements.remove(idx);
        return JsonWriter.jsonb(JsonValue.array(elements));
    }

    /** Delete the value a path names. Implements {@code #-}. */
    public static String deletePath(String json, List<String> path) {
        requireNoNullPathElement(path);
        JsonValue value = jsonbValue(json);
        if (path.isEmpty()) return JsonWriter.jsonb(value);
        if (value.isScalar()) throw new MemgresException("cannot delete path in scalar", "22023");
        return JsonWriter.jsonb(deletePathAt(value, path, 0));
    }

    /**
     * Walk one path step. A step that runs into a scalar simply leaves the value alone — only the
     * target itself being a scalar is an error — but an array reached by a step that is not an
     * integer is a malformed path.
     */
    private static JsonValue deletePathAt(JsonValue value, List<String> path, int at) {
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (value.isArray()) {
            List<JsonValue> elements = value.copyElements();
            int idx = pathIndex(key, at);
            if (idx < 0) idx += elements.size();
            if (idx >= 0 && idx < elements.size()) {
                if (last) elements.remove(idx);
                else if (!elements.get(idx).isScalar()) {
                    elements.set(idx, deletePathAt(elements.get(idx), path, at + 1));
                }
            }
            return JsonValue.array(elements);
        }
        if (value.isObject()) {
            List<String> keys = value.copyKeys();
            List<JsonValue> values = value.copyElements();
            int found = indexOfKey(value, key);
            if (last) {
                if (found >= 0) {
                    keys.remove(found);
                    values.remove(found);
                }
            } else if (found >= 0 && !values.get(found).isScalar()) {
                values.set(found, deletePathAt(values.get(found), path, at + 1));
            }
            return JsonValue.object(keys, values);
        }
        return value;
    }

    private static int indexOfKey(JsonValue object, String key) {
        for (int i = 0; i < object.size(); i++) {
            if (object.keyAt(i).equals(key)) return i;
        }
        return -1;
    }

    /** A path step addressing an array has to be an integer; PG counts the steps from one. */
    private static int pathIndex(String key, int at) {
        try {
            return Integer.parseInt(key.trim());
        } catch (NumberFormatException e) {
            throw new MemgresException("path element at position " + (at + 1)
                    + " is not an integer: \"" + key + "\"", "22P02");
        }
    }

    /**
     * Concatenate two jsonb values. Implements {@code ||}: two objects are merged with the right
     * winning on a shared key; otherwise each side that is not an array is taken as a one-element
     * array and the two are joined.
     */
    public static String concatenate(String left, String right) {
        JsonValue l = jsonbValue(left);
        JsonValue r = jsonbValue(right);
        if (l.isObject() && r.isObject()) {
            List<String> keys = l.copyKeys();
            List<JsonValue> values = l.copyElements();
            keys.addAll(r.keys());
            values.addAll(r.elements());
            // sortedObject keeps the last of a repeated key, which is the right-hand one
            return JsonWriter.jsonb(JsonNormalizer.sortedObject(keys, values));
        }
        List<JsonValue> elements = new ArrayList<JsonValue>();
        if (l.isArray()) elements.addAll(l.elements());
        else elements.add(l);
        if (r.isArray()) elements.addAll(r.elements());
        else elements.add(r);
        return JsonWriter.jsonb(JsonValue.array(elements));
    }

    /** jsonb_set with create_missing left at its default of true. */
    public static String jsonbSet(String json, List<String> path, String newValue) {
        return jsonbSet(json, path, newValue, true);
    }

    /**
     * jsonb_set. An intermediate step is never created: where one is missing the target comes back
     * unchanged, and create_missing only says whether the last step may be added. A negative array
     * index counts from the end.
     */
    public static String jsonbSet(String json, List<String> path, String newValue,
                                  boolean createMissing) {
        JsonValue value = jsonbValue(json);
        if (path.isEmpty()) return JsonWriter.jsonb(value);
        requirePathTarget(value);
        return JsonWriter.jsonb(
                jsonbSetAt(value, path, 0, jsonbValue(newValue), createMissing));
    }

    private static JsonValue jsonbSetAt(JsonValue value, List<String> path, int at,
                                        JsonValue newValue, boolean createMissing) {
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (value.isObject()) {
            List<String> keys = value.copyKeys();
            List<JsonValue> values = value.copyElements();
            int found = indexOfKey(value, key);
            if (last) {
                if (found >= 0) values.set(found, newValue);
                else if (createMissing) { keys.add(key); values.add(newValue); }
                else return value;
                return JsonNormalizer.sortedObject(keys, values);
            }
            if (found < 0 || values.get(found).isScalar()) return value;  // step lands nowhere
            values.set(found, jsonbSetAt(values.get(found), path, at + 1, newValue, createMissing));
            return JsonValue.object(keys, values);
        }
        if (value.isArray()) {
            int idx = pathIndex(key, at);
            List<JsonValue> elements = value.copyElements();
            if (idx < 0) idx += elements.size();
            if (last) {
                if (idx >= 0 && idx < elements.size()) {
                    elements.set(idx, newValue);
                } else if (createMissing) {
                    // A last step off either end adds: before the first, or after the last
                    if (idx < 0) elements.add(0, newValue);
                    else elements.add(newValue);
                }
                return JsonValue.array(elements);
            }
            if (idx >= 0 && idx < elements.size() && !elements.get(idx).isScalar()) {
                elements.set(idx,
                        jsonbSetAt(elements.get(idx), path, at + 1, newValue, createMissing));
                return JsonValue.array(elements);
            }
            return value;   // missing intermediate step: unchanged
        }
        return value;
    }

    /** A path only addresses something inside a container; a scalar has no path to set. */
    private static void requirePathTarget(JsonValue value) {
        if (value.isScalar()) throw new MemgresException("cannot set path in scalar", "22023");
    }

    /**
     * jsonb_insert. A missing intermediate step leaves the target unchanged, inserting at a key
     * that is already there is an error, a negative array index counts from the end, and a position
     * off either end prepends or appends.
     */
    public static String jsonbInsert(String json, List<String> path, String newValue,
                                     boolean insertAfter) {
        JsonValue value = jsonbValue(json);
        if (path.isEmpty()) return JsonWriter.jsonb(value);
        requirePathTarget(value);
        return JsonWriter.jsonb(
                jsonbInsertAt(value, path, 0, jsonbValue(newValue), insertAfter));
    }

    private static JsonValue jsonbInsertAt(JsonValue value, List<String> path, int at,
                                           JsonValue newValue, boolean insertAfter) {
        String key = path.get(at);
        boolean last = at == path.size() - 1;
        if (value.isArray()) {
            int idx = pathIndex(key, at);
            List<JsonValue> elements = value.copyElements();
            if (idx < 0) idx += elements.size();
            if (last) {
                int insertAt = insertAfter ? idx + 1 : idx;
                if (insertAt < 0) insertAt = 0;
                if (insertAt > elements.size()) insertAt = elements.size();
                elements.add(insertAt, newValue);
                return JsonValue.array(elements);
            }
            if (idx >= 0 && idx < elements.size() && !elements.get(idx).isScalar()) {
                elements.set(idx,
                        jsonbInsertAt(elements.get(idx), path, at + 1, newValue, insertAfter));
                return JsonValue.array(elements);
            }
            return value;   // missing intermediate step: unchanged
        }
        if (value.isObject()) {
            List<String> keys = value.copyKeys();
            List<JsonValue> values = value.copyElements();
            int found = indexOfKey(value, key);
            if (last) {
                if (found >= 0) {
                    throw new MemgresException("cannot replace existing key\n"
                            + "  Hint: Try using the function jsonb_set to replace key value.",
                            "22023");
                }
                keys.add(key);
                values.add(newValue);
                return JsonNormalizer.sortedObject(keys, values);
            }
            if (found < 0 || values.get(found).isScalar()) return value;
            values.set(found, jsonbInsertAt(values.get(found), path, at + 1, newValue, insertAfter));
            return JsonValue.object(keys, values);
        }
        return value;
    }

    /** json_strip_nulls in jsonb's shape. */
    public static String stripNulls(String json) {
        return stripNulls(json, false, false);
    }

    /** json_strip_nulls, written the compact way the json type writes a document it rebuilt. */
    public static String stripNulls(String json, boolean compact) {
        return stripNulls(json, compact, false);
    }

    /**
     * The same, with the second argument PostgreSQL's strip_nulls takes. A null member of an object
     * always goes; {@code stripInArrays} says whether a null element of an array goes with it,
     * which is not the default because dropping one moves every element after it.
     *
     * <p>The compact form is the json family's, and it reads the document as json — members in the
     * order they were written, repeated keys and all, because json_strip_nulls is defined as
     * dropping the null members and keeping everything else exactly as it stood.
     */
    public static String stripNulls(String json, boolean compact, boolean stripInArrays) {
        if (compact) {
            return JsonWriter.json(stripNulls(jsonValue(json), stripInArrays));
        }
        return JsonWriter.jsonb(stripNulls(jsonbValue(json), stripInArrays));
    }

    private static JsonValue stripNulls(JsonValue value, boolean stripInArrays) {
        if (value.isObject()) {
            List<String> keys = new ArrayList<String>();
            List<JsonValue> values = new ArrayList<JsonValue>();
            for (int i = 0; i < value.size(); i++) {
                if (value.at(i).isNull()) continue;
                keys.add(value.keyAt(i));
                values.add(stripNulls(value.at(i), stripInArrays));
            }
            return JsonValue.object(keys, values);
        }
        if (value.isArray()) {
            List<JsonValue> kept = new ArrayList<JsonValue>();
            for (int i = 0; i < value.size(); i++) {
                if (stripInArrays && value.at(i).isNull()) continue;
                kept.add(stripNulls(value.at(i), stripInArrays));
            }
            return JsonValue.array(kept);
        }
        return value;
    }

    /** jsonb_pretty: the document laid out one member to a line. */
    public static String pretty(String json) {
        return json == null ? null : JsonWriter.pretty(jsonbValue(json));
    }

    // ---- reading parts of a document out ----

    /** The element at a position, as the document wrote it. A negative index counts from the end. */
    public static String extractArrayElement(String json, int index) {
        if (!isArray(json)) return null;
        List<JsonParser.Member> elements = members(json);
        if (index < 0) index += elements.size();
        return index >= 0 && index < elements.size() ? elements.get(index).text : null;
    }

    /**
     * The value under a key, as the document wrote it. Where a json document wrote one key twice
     * the later member is the one every reader sees, so the search runs backwards.
     */
    public static String extractKey(String json, String key) {
        if (!isObject(json)) return null;
        List<JsonParser.Member> found = members(json);
        for (int i = found.size() - 1; i >= 0; i--) {
            if (found.get(i).key.equals(key)) return found.get(i).text;
        }
        return null;
    }

    /**
     * The members of an object in jsonb's order: shorter keys first, then by their bytes, with one
     * entry per distinct key.
     */
    public static Map<String, String> parseObjectKeys(String json) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        if (!isObject(json)) return result;
        List<JsonParser.Member> found = members(json);
        List<String> keys = new ArrayList<String>(found.size());
        List<JsonValue> values = new ArrayList<JsonValue>(found.size());
        for (int i = 0; i < found.size(); i++) {
            keys.add(found.get(i).key);
            // The text is carried through as written; only the ordering is jsonb's here
            values.add(JsonValue.string(found.get(i).text));
        }
        JsonValue sorted = JsonNormalizer.sortedObject(keys, values);
        for (int i = 0; i < sorted.size(); i++) {
            result.put(sorted.keyAt(i), sorted.at(i).asString());
        }
        return result;
    }

    /** The elements of an array, each as the document wrote it. */
    public static List<String> parseArrayElements(String json) {
        List<String> result = new ArrayList<String>();
        if (!isArray(json)) return result;
        for (JsonParser.Member member : members(json)) result.add(member.text);
        return result;
    }

    /** A jsonb document written the way jsonb writes one. */
    public static String normalizeJsonb(String json) {
        return json == null ? "null" : JsonWriter.jsonb(jsonbValue(json));
    }

    // ---- comparing two documents ----

    /**
     * jsonb's ordering of two documents. jsonb is stored as a value and not as the text it prints
     * as, so neither equality nor ordering can be read off the text: {@code 1} and {@code 1.0} are
     * one value spelled two ways, and a container is ordered by how many members it has before any
     * member is looked at, which puts {@code [3]} before {@code [1, 2]}.
     */
    public static int compareJsonb(String left, String right) {
        return JsonValue.compareDocuments(jsonbValue(left), jsonbValue(right));
    }

    /**
     * A text form of a document in which equal values have equal texts. The canonical text almost
     * has that property already — its members are sorted and its keys are unique — and the one
     * thing left is how a number was spelled, so the trailing zeros come off.
     *
     * <p>This is what the operations that put values in a set need, where {@link #compareJsonb}
     * serves the ones that compare two values at a time.
     */
    public static String jsonbKey(String json) {
        return JsonWriter.jsonb(stripNumberScale(jsonbValue(json)));
    }

    private static JsonValue stripNumberScale(JsonValue value) {
        switch (value.kind()) {
            case JsonValue.NUMBER:
                return JsonValue.number(value.asNumber().stripTrailingZeros());
            case JsonValue.ARRAY: {
                List<JsonValue> elements = new ArrayList<JsonValue>(value.size());
                for (int i = 0; i < value.size(); i++) elements.add(stripNumberScale(value.at(i)));
                return JsonValue.array(elements);
            }
            case JsonValue.OBJECT: {
                List<JsonValue> values = new ArrayList<JsonValue>(value.size());
                for (int i = 0; i < value.size(); i++) values.add(stripNumberScale(value.at(i)));
                return JsonValue.object(value.copyKeys(), values);
            }
            default:
                return value;
        }
    }
}
