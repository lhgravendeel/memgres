package com.memgres.engine;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Writes a JSON value back out, in each of the shapes PostgreSQL writes one.
 *
 * <p>The three differ only in spacing. jsonb writes {@code {"a": 1}}; a walk that rebuilds a json
 * document writes it with no spaces at all, which is what {@code row_to_json} and
 * {@code json_strip_nulls} produce; and the SQL/JSON constructors put a space either side of the
 * colon. {@link #pretty} is the fourth shape, and the only one that breaks lines.
 *
 * <p>Escaping is the part that was being done by hand in several places and correctly in none of
 * them. A control character has no literal spelling in JSON, so {@code chr(1)} written into a
 * document has to come back out as {@code \u0001}; writing it raw produced text that no reader
 * could take back, and dropping it lost the value. Object keys were being appended without any
 * escaping at all, so a key holding a quote closed the string it was in.
 */
final class JsonWriter {

    private JsonWriter() {
    }

    /** The text jsonb prints as: a space after a colon and after a comma. */
    static String jsonb(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value, ": ", ", ", false);
        return sb.toString();
    }

    /** The text a rebuilt json document prints as: no spacing anywhere. */
    static String json(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value, ":", ",", false);
        return sb.toString();
    }

    /** The text the SQL/JSON constructors print as: a space either side of the colon. */
    static String jsonConstructed(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value, " : ", ", ", false);
        return sb.toString();
    }

    /**
     * jsonb_pretty's shape: one member to a line, indented four spaces a level. A container opens
     * on the line its key is on and closes on a line of its own, so an empty one takes two lines.
     */
    static String pretty(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        write(sb, value, ": ", ", ", true);
        return sb.toString();
    }

    /**
     * Written with a stack rather than by recursion: a document may nest as deeply as the parser
     * will take it, which is far deeper than the Java stack goes.
     */
    private static void write(StringBuilder sb, JsonValue root,
                              String keySep, String itemSep, boolean pretty) {
        Deque<Frame> open = new ArrayDeque<Frame>();
        JsonValue value = root;
        for (;;) {
            if (value.isScalar()) {
                appendScalar(sb, value);
            } else {
                sb.append(value.isObject() ? '{' : '[');
                open.push(new Frame(value));
            }
            // The value just written is finished. Move the innermost container on, closing every
            // one that has nothing left in it.
            for (;;) {
                if (open.isEmpty()) return;
                Frame frame = open.peek();
                if (frame.next < frame.value.size()) {
                    if (frame.next > 0) sb.append(pretty ? "," : itemSep);
                    if (pretty) indent(sb, open.size());
                    if (frame.value.isObject()) {
                        appendString(sb, frame.value.keyAt(frame.next));
                        sb.append(keySep);
                    }
                    value = frame.value.at(frame.next++);
                    break;
                }
                open.pop();
                if (pretty) indent(sb, open.size());
                sb.append(frame.value.isObject() ? '}' : ']');
            }
        }
    }

    private static void indent(StringBuilder sb, int level) {
        sb.append('\n');
        for (int i = 0; i < level * 4; i++) sb.append(' ');
    }

    private static void appendScalar(StringBuilder sb, JsonValue value) {
        switch (value.kind()) {
            case JsonValue.NULL: sb.append("null"); break;
            case JsonValue.BOOLEAN: sb.append(value.asBoolean() ? "true" : "false"); break;
            case JsonValue.NUMBER: sb.append(value.numberText()); break;
            default: appendString(sb, value.asString());
        }
    }

    /** A text value as a JSON string literal, escaping what JSON cannot hold literally. */
    static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        appendString(sb, s);
        return sb.toString();
    }

    static void appendString(StringBuilder sb, String s) {
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
                    // Anything else below a space has no spelling of its own in JSON
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
    }

    /** A container part-way through being written. */
    private static final class Frame {
        final JsonValue value;
        int next;

        Frame(JsonValue value) {
            this.value = value;
        }
    }
}
