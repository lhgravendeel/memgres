package com.memgres.engine;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

/**
 * What jsonb keeps of a document that json keeps whole.
 *
 * <p>jsonb is not the text it was given. Its members are held in an order of its own, one key
 * appears once however many times the document wrote it, and its numbers are numerics rather than
 * the digits somebody typed. Both rules were being applied to the text instead of to the value:
 * keys were ordered by how many Java chars they take rather than how many bytes PostgreSQL stores
 * them in, which puts every key outside ASCII in the wrong place, and a number keeping an exponent
 * was expanded without asking first whether a numeric has room for what it names.
 */
final class JsonNormalizer {

    private JsonNormalizer() {
    }

    /**
     * PostgreSQL's key order: shorter keys first, and keys of one length by their bytes.
     *
     * <p>The length and the bytes are both UTF-8's, which is how the server stores them. Counting
     * Java chars instead agrees for ASCII and disagrees for everything else — {@code "é"} is one
     * char and two bytes, so it sorted with the one-character keys rather than with the pairs.
     */
    static final Comparator<byte[]> KEY_ORDER = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] a, byte[] b) {
            if (a.length != b.length) return a.length < b.length ? -1 : 1;
            for (int i = 0; i < a.length; i++) {
                // The bytes are compared unsigned, as memcmp compares them
                int cmp = (a[i] & 0xff) - (b[i] & 0xff);
                if (cmp != 0) return cmp;
            }
            return 0;
        }
    };

    /**
     * An object holding these members in jsonb's order, with one entry per distinct key. Where a
     * document wrote a key twice, the later member is the one jsonb keeps.
     */
    static JsonValue sortedObject(List<String> keys, List<JsonValue> values) {
        int n = keys.size();
        final byte[][] bytes = new byte[n][];
        for (int i = 0; i < n; i++) bytes[i] = keys.get(i).getBytes(StandardCharsets.UTF_8);
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) order[i] = i;
        Arrays.sort(order, new Comparator<Integer>() {
            @Override
            public int compare(Integer i, Integer j) {
                int cmp = KEY_ORDER.compare(bytes[i], bytes[j]);
                // Equal keys stay in the order they were written, so the last one is the last seen
                return cmp != 0 ? cmp : Integer.compare(i, j);
            }
        });
        List<String> outKeys = new ArrayList<String>(n);
        List<JsonValue> outValues = new ArrayList<JsonValue>(n);
        for (int i = 0; i < n; i++) {
            int at = order[i];
            if (i + 1 < n && KEY_ORDER.compare(bytes[at], bytes[order[i + 1]]) == 0) continue;
            outKeys.add(keys.get(at));
            outValues.add(values.get(at));
        }
        return JsonValue.object(outKeys, outValues);
    }

    /**
     * The same document as jsonb would store it. Walked with a stack rather than by recursion,
     * because a document may nest deeper than the Java stack goes.
     */
    static JsonValue normalize(JsonValue root) {
        if (root.isScalar()) return scalar(root);
        Deque<Build> open = new ArrayDeque<Build>();
        open.push(new Build(root));
        JsonValue result = null;
        while (!open.isEmpty()) {
            Build top = open.peek();
            if (top.next < top.source.size()) {
                JsonValue child = top.source.at(top.next++);
                if (child.isScalar()) top.done.add(scalar(child));
                else open.push(new Build(child));
                continue;
            }
            open.pop();
            JsonValue built = top.source.isArray()
                    ? JsonValue.array(top.done)
                    : sortedObject(top.source.keys(), top.done);
            if (open.isEmpty()) result = built;
            else open.peek().done.add(built);
        }
        return result;
    }

    /** A scalar as jsonb holds it: a number is the numeric it means, whatever it was spelled as. */
    private static JsonValue scalar(JsonValue value) {
        if (value.kind() != JsonValue.NUMBER) return value;
        requireNumericRange(value.asNumber());
        return JsonValue.number(value.asNumber());
    }

    /**
     * The range a numeric has room for, measured against PostgreSQL 18: a value below 10^131072
     * with no more than 16383 digits after the point. jsonb stores its numbers as numerics, so a
     * document naming one outside that is refused rather than expanded into the megabytes of
     * digits it asks for.
     */
    static void requireNumericRange(BigDecimal value) {
        if (value.scale() > 16383 || value.precision() - value.scale() > 131072) {
            throw new MemgresException("value overflows numeric format", "22003");
        }
    }

    /** A container part-way through being rebuilt. */
    private static final class Build {
        final JsonValue source;
        final List<JsonValue> done;
        int next;

        Build(JsonValue source) {
            this.source = source;
            this.done = new ArrayList<JsonValue>(source.size());
        }
    }
}
