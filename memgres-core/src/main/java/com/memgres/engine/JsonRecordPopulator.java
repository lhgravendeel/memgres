package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * The functions that make a record out of a JSON object: {@code json_populate_record},
 * {@code json_to_record}, the {@code _recordset} forms of both, and the jsonb spelling of all four.
 *
 * <p>All of them do the same two things — decide that the document is the shape the function takes,
 * and lay one JSON object over a record whose fields are already named and typed — so all of them
 * do it here. Each used to split the document on its own commas and colons and guess at what a
 * value was, which meant a brace inside a string ended the object, an escape stayed as the two
 * characters that spell it, a field arrived as whatever Java made of its text rather than as the
 * type the field is declared, and a document of the wrong shape altogether was quietly read as
 * though it were right.
 */
final class JsonRecordPopulator {

    private final AstExecutor executor;

    JsonRecordPopulator(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * The members of the object a record-returning populator fills from.
     *
     * <p>PostgreSQL names the routine behind all four functions here rather than the one that was
     * written, so the complaint reads the same whichever was called.
     */
    static List<JsonParser.Member> objectMembers(String json) {
        int kind = JsonParser.kindOf(json);
        if (kind != JsonValue.OBJECT) {
            throw new MemgresException("cannot call populate_composite on "
                    + (kind == JsonValue.ARRAY ? "an array" : "a scalar"), "22023");
        }
        return JsonParser.membersOf(json);
    }

    /**
     * The members of each object a set-returning populator makes a row from: the document must be
     * an array, and every element of it an object.
     */
    static List<List<JsonParser.Member>> objectsMembers(String function, String json) {
        int kind = JsonParser.kindOf(json);
        if (kind != JsonValue.ARRAY) {
            // The json spelling says which of the two it was handed; the jsonb spelling says only
            // that it was not an array
            String what = function.startsWith("jsonb")
                    ? "a non-array" : kind == JsonValue.OBJECT ? "an object" : "a scalar";
            throw new MemgresException("cannot call " + function + " on " + what, "22023");
        }
        List<List<JsonParser.Member>> rows = new ArrayList<List<JsonParser.Member>>();
        for (JsonParser.Member element : JsonParser.membersOf(json)) {
            if (JsonParser.kindOf(element.text) != JsonValue.OBJECT) {
                throw new MemgresException(
                        "argument of " + function + " must be an array of objects", "22023");
            }
            rows.add(JsonParser.membersOf(element.text));
        }
        return rows;
    }

    /**
     * One field of the record: the value the object holds under the field's name, read as the
     * field's type, or — for a name the object does not carry at all — whatever the record being
     * filled already held there. A name written twice is taken at its last mention, which is the
     * one every other reader of the document sees.
     *
     * @param typeName the field's type as it was declared, or null for one memgres cannot name,
     *                 whose value is then left as the text the document wrote
     */
    Object fieldValue(List<JsonParser.Member> members, String name, String typeName, Object base) {
        String text = null;
        for (JsonParser.Member member : members) {
            if (name.equals(member.key)) text = member.text;
        }
        if (text == null) return base;
        int kind = JsonParser.kindOf(text);
        // A JSON null is the field holding nothing, and does not fall back on the base record
        if (kind == JsonValue.NULL) return null;
        if (typeName == null) return kind == JsonValue.STRING ? decoded(text) : text;
        if (isArrayType(typeName)) return arrayField(name, kind, text, typeName);
        // Every other type reads the value the way it reads text written in a query: a string as
        // the characters it stands for, and anything else as the document spelled it. That is what
        // makes "yes" a boolean, " 7 " an integer, and 1.9 no integer at all.
        return executor.castEvaluator.applyCast(
                kind == JsonValue.STRING ? decoded(text) : text, typeName);
    }

    /**
     * An array field, which a JSON array fills element by element. A string is read as an array
     * literal, exactly as text written in a query would be; anything else is not an array and
     * PostgreSQL says so rather than making a one-element array of it.
     */
    private Object arrayField(String name, int kind, String text, String typeName) {
        if (kind == JsonValue.STRING) {
            return executor.castEvaluator.applyCast(decoded(text), typeName);
        }
        if (kind != JsonValue.ARRAY) {
            MemgresException e = new MemgresException("expected JSON array", "22P02");
            e.setHint("See the value of key \"" + name + "\".");
            throw e;
        }
        return executor.castEvaluator.applyCast(elementValues(text), typeName);
    }

    /** The elements of a JSON array as the values a cast to an array type takes. */
    private List<Object> elementValues(String text) {
        List<JsonParser.Member> elements = JsonParser.membersOf(text);
        List<Object> values = new ArrayList<Object>(elements.size());
        for (JsonParser.Member element : elements) {
            int kind = JsonParser.kindOf(element.text);
            if (kind == JsonValue.NULL) values.add(null);
            else if (kind == JsonValue.ARRAY) values.add(elementValues(element.text));
            else if (kind == JsonValue.STRING) values.add(decoded(element.text));
            else values.add(element.text);
        }
        return values;
    }

    /** The characters a JSON string stands for, which are not the characters that spell it. */
    private static String decoded(String text) {
        return JsonParser.parse(text).asString();
    }

    /** Whether a declared type is an array of something, and so is filled from a JSON array. */
    private static boolean isArrayType(String typeName) {
        String name = typeName.trim();
        if (name.endsWith("]")) return true;
        DataType type = DataType.fromPgName(name.toLowerCase(java.util.Locale.ROOT));
        return type != null && DataType.isArrayType(type);
    }
}
