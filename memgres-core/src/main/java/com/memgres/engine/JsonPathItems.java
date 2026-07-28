package com.memgres.engine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The jsonpath item methods -- the {@code .type()}, {@code .size()}, {@code .abs()} and friends
 * that a path may end in.
 *
 * <p>Two rules run through all of them. lax mode, the default, unwraps an array before applying a
 * method that wants a single value, so {@code $.abs()} over {@code [1,-2]} answers twice; strict
 * mode does not, and complains that the method was handed an array. And a method applied to the
 * wrong kind of value is an error rather than an empty result, because answering nothing would be
 * indistinguishable from a path that simply matched nothing.
 */
final class JsonPathItems {

    private static final Set<String> NAMES = new HashSet<String>(Arrays.asList(
            "type", "size", "abs", "floor", "ceiling", "double", "keyvalue", "datetime"));

    private JsonPathItems() {
    }

    /** True when a path segment names an item method rather than an object key. */
    static boolean isMethod(String segment) {
        if (segment == null || !segment.endsWith("()")) return false;
        return NAMES.contains(segment.substring(0, segment.length() - 2).toLowerCase());
    }

    /** Applies the method named by {@code segment} to every node, producing the next node list. */
    static List<String> apply(String segment, List<String> nodes, boolean strict) {
        String method = segment.substring(0, segment.length() - 2).toLowerCase();
        List<String> out = new ArrayList<String>();
        for (String raw : nodes) {
            String node = raw.trim();
            // .type() and .size() describe the item they are given, so an array is the answer
            // rather than something to look inside.
            if (method.equals("type")) {
                out.add(JsonOperations.quote(typeOf(node)));
            } else if (method.equals("size")) {
                out.add(Integer.toString(JsonOperations.isArray(node)
                        ? JsonOperations.parseArrayElements(node).size() : 1));
            } else if (!strict && JsonOperations.isArray(node)) {
                for (String element : JsonOperations.parseArrayElements(node)) {
                    applyScalar(method, element.trim(), out);
                }
            } else {
                applyScalar(method, node, out);
            }
        }
        return out;
    }

    private static void applyScalar(String method, String node, List<String> out) {
        if (method.equals("keyvalue")) {
            applyKeyValue(node, out);
        } else if (method.equals("datetime")) {
            out.add(datetime(node));
        } else if (method.equals("double")) {
            out.add(toDouble(node));
        } else {
            out.add(numeric(method, node));
        }
    }

    /**
     * Each pair becomes an object of its own. PG's {@code id} is the offset of the containing
     * object inside the stored jsonb, which is 0 for the document itself -- the only case whose
     * value is reproducible without PG's binary layout.
     */
    private static void applyKeyValue(String node, List<String> out) {
        if (!JsonOperations.isObject(node)) {
            throw new MemgresException(
                    "jsonpath item method .keyvalue() can only be applied to an object", "2203C");
        }
        Map<String, String> pairs = JsonOperations.parseObjectKeys(node);
        for (Map.Entry<String, String> entry : pairs.entrySet()) {
            out.add("{\"id\": 0, \"key\": " + JsonOperations.quote(entry.getKey())
                    + ", \"value\": " + entry.getValue().trim() + "}");
        }
    }

    private static String numeric(String method, String node) {
        BigDecimal value = asNumber(node);
        if (value == null) {
            throw new MemgresException("jsonpath item method ." + method
                    + "() can only be applied to a numeric value", "22036");
        }
        if (method.equals("abs")) return value.abs().toPlainString();
        if (method.equals("floor")) return value.setScale(0, java.math.RoundingMode.FLOOR).toPlainString();
        return value.setScale(0, java.math.RoundingMode.CEILING).toPlainString();
    }

    /** .double() also accepts a JSON string, and names the string when it does not parse. */
    private static String toDouble(String node) {
        BigDecimal value = asNumber(node);
        if (value == null && JsonOperations.isString(node)) {
            String text = JsonOperations.jsonValueToText(node);
            try {
                value = new BigDecimal(text.trim());
            } catch (NumberFormatException e) {
                throw new MemgresException("argument \"" + text + "\" of jsonpath item method"
                        + " .double() is invalid for type double precision", "22036");
            }
        }
        if (value == null) {
            throw new MemgresException("jsonpath item method .double() can only be applied to"
                    + " a string or numeric value", "22036");
        }
        double d = value.doubleValue();
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new MemgresException("argument \"" + node + "\" of jsonpath item method"
                    + " .double() is invalid for type double precision", "22036");
        }
        return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
    }

    /**
     * A date or time string keeps the shape it came in with -- only the two spellings SQL allows
     * for the same instant are normalised: the space between date and time becomes a T, and a
     * trailing Z becomes the offset it stands for.
     */
    static String datetime(String node) {
        if (!JsonOperations.isString(node)) return node;
        String text = JsonOperations.jsonValueToText(node);
        return JsonOperations.quote(normalizeDatetime(text));
    }

    /** The same normalisation, applied to the bare text of a datetime literal. */
    static String normalizeDatetime(String text) {
        String value = text.trim();
        int space = value.indexOf(' ');
        if (space > 0 && value.length() > space + 1 && Character.isDigit(value.charAt(space + 1))) {
            value = value.substring(0, space) + "T" + value.substring(space + 1);
        }
        if (value.endsWith("Z") && value.length() > 1) {
            value = value.substring(0, value.length() - 1) + "+00:00";
        }
        return value;
    }

    private static BigDecimal asNumber(String node) {
        if (node.isEmpty() || JsonOperations.isString(node)) return null;
        try {
            return new BigDecimal(node);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String typeOf(String node) {
        if (node.equals("null")) return "null";
        if (node.equals("true") || node.equals("false")) return "boolean";
        if (JsonOperations.isObject(node)) return "object";
        if (JsonOperations.isArray(node)) return "array";
        if (JsonOperations.isString(node)) return "string";
        return "number";
    }
}
