package com.memgres.engine;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * One JSON value, held as what it is rather than as the text it prints as.
 *
 * <p>json and jsonb were both plain Java strings, so every question about them had to be answered
 * by looking at characters: whether two numbers are the same number, which of two keys sorts first,
 * what a string really holds once its escapes are read. Those answers came out wrong often enough
 * that the text had to stop being the value. A parsed document is a tree of these, and the two
 * types differ only in how one is built and written back:
 *
 * <ul>
 *   <li>json keeps what was written — every member in its original order, duplicate keys and all,
 *       and every number spelled the way the document spelled it.
 *   <li>jsonb keeps what was meant — members sorted by key and duplicates resolved to the last
 *       one written, and every number converted to a numeric, so {@code 1} and {@code 1.0} are one
 *       value that happens to print two ways.
 * </ul>
 *
 * <p>The kind constants are numbered in PostgreSQL's own jsonb sort order, so comparing two values
 * of different kinds is comparing their kinds.
 */
public final class JsonValue {

    public static final int NULL = 0;
    public static final int STRING = 1;
    public static final int NUMBER = 2;
    public static final int BOOLEAN = 3;
    public static final int ARRAY = 4;
    public static final int OBJECT = 5;
    /**
     * A date or time, which no document can hold and only a jsonpath can make -- so it is numbered
     * past the kinds jsonb sorts, where it can never turn up.
     */
    public static final int DATETIME = 6;

    public static final JsonValue JSON_NULL = new JsonValue(NULL, false, null, null, null, null);
    public static final JsonValue TRUE = new JsonValue(BOOLEAN, true, null, null, null, null);
    public static final JsonValue FALSE = new JsonValue(BOOLEAN, false, null, null, null, null);

    private final int kind;
    private final boolean flag;
    /** The date or time of a {@link #DATETIME}, and null for every other kind. */
    private JsonPathDatetime moment;
    /**
     * A string's characters, or a number as the document spelled it. Null for a number that has no
     * spelling of its own, whose text is worked out from the numeric the first time it is asked
     * for — a numeric may name a hundred thousand digits, which are not worth writing out for a
     * value that is only ever compared.
     */
    private String text;
    private final BigDecimal number;
    /** An object's keys, in the order this value keeps them. */
    private final List<String> keys;
    /** An object's values under {@link #keys}, or an array's elements. */
    private final List<JsonValue> items;

    private JsonValue(int kind, boolean flag, String text, BigDecimal number,
                      List<String> keys, List<JsonValue> items) {
        this.kind = kind;
        this.flag = flag;
        this.text = text;
        this.number = number;
        this.keys = keys;
        this.items = items;
    }

    public static JsonValue string(String value) {
        return new JsonValue(STRING, false, value, null, null, null);
    }

    public static JsonValue bool(boolean value) {
        return value ? TRUE : FALSE;
    }

    /**
     * A number as the document spelled it. The {@link BigDecimal} is what the spelling means, and
     * is what two numbers are compared by; the text is what a json document writes back.
     */
    public static JsonValue number(String spelling, BigDecimal value) {
        return new JsonValue(NUMBER, false, spelling, value, null, null);
    }

    /** A number with no spelling of its own, which writes back as its numeric form. */
    public static JsonValue number(BigDecimal value) {
        return new JsonValue(NUMBER, false, null, value, null, null);
    }

    /**
     * A date or time. It prints as the string it would have been written as, so a query that ends
     * on one hands back a jsonb string; what it is only shows while the path is still running.
     */
    static JsonValue datetime(JsonPathDatetime value) {
        JsonValue v = new JsonValue(DATETIME, false, value.text(), null, null, null);
        v.moment = value;
        return v;
    }

    JsonPathDatetime asDatetime() {
        return moment;
    }

    public static JsonValue array(List<JsonValue> elements) {
        return new JsonValue(ARRAY, false, null, null, null, elements);
    }

    /** An object of the members given, kept in the order given. */
    public static JsonValue object(List<String> keys, List<JsonValue> values) {
        return new JsonValue(OBJECT, false, null, null, keys, values);
    }

    public int kind() {
        return kind;
    }

    public boolean isNull() {
        return kind == NULL;
    }

    public boolean isObject() {
        return kind == OBJECT;
    }

    public boolean isArray() {
        return kind == ARRAY;
    }

    public boolean isScalar() {
        return kind != OBJECT && kind != ARRAY;
    }

    public boolean asBoolean() {
        return flag;
    }

    /** A string's characters. */
    public String asString() {
        return text;
    }

    /**
     * A number as the document spelled it, or — for one converted to a numeric — as a numeric
     * prints. numeric has no exponent notation on output, so the plain form is the whole of it.
     */
    public String numberText() {
        if (text == null) text = number.toPlainString();
        return text;
    }

    public BigDecimal asNumber() {
        return number;
    }

    /** The member count of an object, the element count of an array, and zero for a scalar. */
    public int size() {
        return items == null ? 0 : items.size();
    }

    public JsonValue at(int index) {
        return items.get(index);
    }

    public String keyAt(int index) {
        return keys.get(index);
    }

    public List<String> keys() {
        return keys == null ? Collections.<String>emptyList() : keys;
    }

    public List<JsonValue> elements() {
        return items == null ? Collections.<JsonValue>emptyList() : items;
    }

    /**
     * The value stored under a key, or null when the object has no such member. Where a json
     * document wrote one key twice the later member is the one every reader sees, so the search
     * runs backwards.
     */
    public JsonValue member(String key) {
        if (kind != OBJECT) return null;
        for (int i = keys.size() - 1; i >= 0; i--) {
            if (keys.get(i).equals(key)) return items.get(i);
        }
        return null;
    }

    public boolean hasMember(String key) {
        return kind == OBJECT && keys.contains(key);
    }

    /** PostgreSQL's name for this value's kind, as json_typeof gives it. */
    public String typeName() {
        switch (kind) {
            case NULL: return "null";
            case STRING: return "string";
            case NUMBER: return "number";
            case BOOLEAN: return "boolean";
            case ARRAY: return "array";
            case DATETIME: return moment.sqlTypeName();
            default: return "object";
        }
    }

    /**
     * The same value with its members sorted and its numbers converted, which is what jsonb keeps.
     * A document parsed as json can be handed to jsonb this way without being written out and read
     * back in.
     */
    public JsonValue asJsonb() {
        return JsonNormalizer.normalize(this);
    }

    /**
     * PostgreSQL's jsonb ordering. Values of different kinds are ordered by kind; within a kind an
     * array or an object is ordered by how many members it has before any member is looked at, and
     * only then member by member.
     *
     * <p>Written with a stack rather than by recursion because the documents this walks may nest
     * as deeply as the parser will take them, which is deeper than the Java stack goes.
     */
    /**
     * The ordering of two whole documents, which is {@link #compare} with one thing added.
     *
     * <p>A document that is nothing but a scalar is held as an array of that one scalar, and two
     * arrays are ordered by how many elements they hold before either is looked into. So a
     * document that is the empty array sorts before every scalar document -- it has no elements
     * and a scalar has one -- while an array of one or more sorts after them, the way the kinds
     * alone would have it. Inside a document there is no such wrapper and a scalar is compared
     * by its kind, so this belongs at the top and nowhere else.
     */
    public static int compareDocuments(JsonValue a, JsonValue b) {
        if (isEmptyArray(a) && isScalar(b)) return -1;
        if (isScalar(a) && isEmptyArray(b)) return 1;
        return compare(a, b);
    }

    private static boolean isEmptyArray(JsonValue value) {
        return value.kind == ARRAY && value.size() == 0;
    }

    private static boolean isScalar(JsonValue value) {
        return value.kind != ARRAY && value.kind != OBJECT;
    }

    public static int compare(JsonValue a, JsonValue b) {
        Deque<JsonValue[]> work = new ArrayDeque<JsonValue[]>();
        work.push(new JsonValue[]{a, b});
        while (!work.isEmpty()) {
            JsonValue[] pair = work.pop();
            JsonValue x = pair[0];
            JsonValue y = pair[1];
            if (x.kind != y.kind) return x.kind < y.kind ? -1 : 1;
            int cmp;
            switch (x.kind) {
                case NULL:
                    continue;
                case BOOLEAN:
                    cmp = Boolean.compare(x.flag, y.flag);
                    break;
                case NUMBER:
                    cmp = x.number.compareTo(y.number);
                    break;
                case STRING:
                case DATETIME:
                    cmp = x.text.compareTo(y.text);
                    break;
                case ARRAY:
                    cmp = Integer.compare(x.size(), y.size());
                    if (cmp == 0) pushPairs(work, x.items, y.items);
                    break;
                default:
                    cmp = Integer.compare(x.size(), y.size());
                    if (cmp == 0) {
                        // A pair is its key and then its value, so the two go on the stack together
                        for (int i = x.size() - 1; i >= 0; i--) {
                            work.push(new JsonValue[]{x.items.get(i), y.items.get(i)});
                            work.push(new JsonValue[]{string(x.keys.get(i)), string(y.keys.get(i))});
                        }
                    }
                    break;
            }
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    private static void pushPairs(Deque<JsonValue[]> work, List<JsonValue> xs, List<JsonValue> ys) {
        for (int i = xs.size() - 1; i >= 0; i--) {
            work.push(new JsonValue[]{xs.get(i), ys.get(i)});
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof JsonValue && compare(this, (JsonValue) other) == 0;
    }

    @Override
    public int hashCode() {
        // Two values that compare equal must hash alike, so a number hashes by what it means and
        // not by how it was spelled: 1 and 1.0 are one value.
        switch (kind) {
            case NULL: return 0;
            case BOOLEAN: return flag ? 1 : 2;
            case NUMBER: return number.stripTrailingZeros().hashCode();
            case STRING: case DATETIME: return text.hashCode();
            default: return kind * 31 + size();
        }
    }

    /** The jsonb text of this value, which is what {@code toString} is wanted for everywhere. */
    @Override
    public String toString() {
        return JsonWriter.jsonb(this);
    }

    /** A mutable copy of an array's elements, for the operations that build a new array. */
    public List<JsonValue> copyElements() {
        return new ArrayList<JsonValue>(elements());
    }

    /** A mutable copy of an object's keys, for the operations that build a new object. */
    public List<String> copyKeys() {
        return new ArrayList<String>(keys());
    }
}
