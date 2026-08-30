package com.memgres.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A jsonpath applied to a document.
 *
 * <p>Two rules run through the whole of it and are worth stating once rather than at every step.
 *
 * <p>The first is lax mode, which is the default and which exists so that a path written for one
 * document also works on the array of them. Before a step that wants a single item, an array is
 * opened and its elements are handed on one at a time; before a step that wants an array, a single
 * item is wrapped in one. Both happen exactly one level deep, so {@code $.a} finds nothing in
 * {@code [[{"a":1}]]} — the outer array is opened, the inner one is not an object, and there is
 * nothing to read. strict mode does neither and complains instead, which is what makes it useful:
 * the point of {@code strict} is to be told that the document is not the shape you thought.
 *
 * <p>The second is that a filter answers true, false or unknown rather than raising. A predicate
 * asks a question about an item, and an item the question does not apply to answers "no", not
 * "this document is malformed" — so an error from evaluating either side of a comparison, whether
 * it is a missing key or a division by zero, becomes unknown and the item is simply not selected.
 * That is also why {@code strict} inside a filter is not a contradiction: the error is raised and
 * then caught, which is different from never having looked.
 */
final class JsonPathEvaluator {

    /**
     * The scale PostgreSQL's numeric division reaches for: at least this many significant digits,
     * rounded out to whole four-digit groups.
     */
    private static final int MIN_SIG_DIGITS = 16;
    private static final int NBASE_DIGITS = 4;

    private static final List<String> DATETIME_METHODS = Collections.unmodifiableList(
            Arrays.asList("date", "time", "time_tz", "timestamp", "timestamp_tz", "datetime"));

    private final boolean strict;
    private final JsonValue root;
    private final JsonValue vars;
    private final boolean tzAllowed;
    /**
     * The subscript one past the end of the array currently being subscripted, which is what
     * {@code last} means. The parser has already refused a {@code last} outside a subscript, so
     * this is set whenever one can be read.
     */
    private int lastSubscript;
    /** Where in the document each object sits, filled in the first time {@code .keyvalue()} asks. */
    private java.util.IdentityHashMap<JsonValue, Integer> objectIds;

    private JsonPathEvaluator(boolean strict, JsonValue root, JsonValue vars, boolean tzAllowed) {
        this.strict = strict;
        this.root = root;
        this.vars = vars;
        this.tzAllowed = tzAllowed;
    }

    /**
     * The items {@code path} selects from {@code document}.
     *
     * <p>A path that is a predicate rather than a sequence of steps selects one item: the answer,
     * with unknown written as JSON null. That is how {@code jsonb_path_query} comes to return
     * {@code false} for {@code $.a > 2} rather than nothing at all.
     *
     * @param vars      the object {@code $name} references are read out of, or null where the call
     *                  passed none
     * @param tzAllowed whether the caller was one of the {@code _tz} functions
     */
    static List<JsonValue> query(JsonValue document, JsonPath path, JsonValue vars,
                                 boolean tzAllowed) {
        JsonPathEvaluator e = new JsonPathEvaluator(path.strict, document, vars, tzAllowed);
        if (path.body.isPredicate()) {
            Boolean answer = e.predicate(path.body, document);
            return Collections.singletonList(
                    answer == null ? JsonValue.JSON_NULL : JsonValue.bool(answer));
        }
        return e.eval(path.body, document);
    }

    // ---------------------------------------------------------------- the steps

    private List<JsonValue> eval(JsonPath.Node node, JsonValue current) {
        if (node instanceof JsonPath.Root) return Collections.singletonList(root);
        if (node instanceof JsonPath.Current) return Collections.singletonList(current);
        if (node instanceof JsonPath.Literal) {
            return Collections.singletonList(((JsonPath.Literal) node).value);
        }
        if (node instanceof JsonPath.Last) {
            return Collections.singletonList(
                    JsonValue.number(BigDecimal.valueOf(lastSubscript)));
        }
        if (node instanceof JsonPath.Variable) {
            return Collections.singletonList(variable(((JsonPath.Variable) node).name));
        }
        if (node instanceof JsonPath.Member) return member((JsonPath.Member) node, current);
        if (node instanceof JsonPath.MemberAll) return memberAll((JsonPath.MemberAll) node, current);
        if (node instanceof JsonPath.AnyLevel) return anyLevel((JsonPath.AnyLevel) node, current);
        if (node instanceof JsonPath.IndexAll) return indexAll((JsonPath.IndexAll) node, current);
        if (node instanceof JsonPath.Index) return index((JsonPath.Index) node, current);
        if (node instanceof JsonPath.Method) return method((JsonPath.Method) node, current);
        if (node instanceof JsonPath.Filter) return filter((JsonPath.Filter) node, current);
        if (node instanceof JsonPath.Binary) return binary((JsonPath.Binary) node, current);
        if (node instanceof JsonPath.Unary) return unary((JsonPath.Unary) node, current);
        // A predicate written where an item was expected still produces one: its answer.
        Boolean answer = predicate(node, current);
        return Collections.singletonList(
                answer == null ? JsonValue.JSON_NULL : JsonValue.bool(answer));
    }

    private JsonValue variable(String name) {
        JsonValue value = vars == null ? null : vars.member(name);
        if (value == null) {
            throw new MemgresException("could not find jsonpath variable \"" + name + "\"", "42704");
        }
        return value;
    }

    private List<JsonValue> member(JsonPath.Member node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : unwrap(eval(node.input, current))) {
            if (!item.isObject()) {
                if (strict) {
                    throw new MemgresException(
                            "jsonpath member accessor can only be applied to an object", "2203A");
                }
                continue;
            }
            if (item.hasMember(node.key)) {
                out.add(item.member(node.key));
            } else if (strict) {
                throw new MemgresException(
                        "JSON object does not contain key \"" + node.key + "\"", "2203A");
            }
        }
        return out;
    }

    private List<JsonValue> memberAll(JsonPath.MemberAll node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : unwrap(eval(node.input, current))) {
            if (item.isObject()) {
                out.addAll(item.elements());
            } else if (strict) {
                throw new MemgresException(
                        "jsonpath wildcard member accessor can only be applied to an object",
                        "2203C");
            }
        }
        return out;
    }

    /**
     * {@code .**} is the item itself and then everything inside it, however deep. It is the one
     * accessor that does not unwrap in lax mode, because the array it would unwrap is a level of
     * its own answer.
     */
    private List<JsonValue> anyLevel(JsonPath.AnyLevel node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : eval(node.input, current)) {
            descend(item, 0, node.from, node.to, out);
        }
        return out;
    }

    private void descend(JsonValue item, int level, int from, int to, List<JsonValue> out) {
        if (level > to) return;
        if (level >= from) out.add(item);
        if (item.isArray() || item.isObject()) {
            for (JsonValue child : item.elements()) descend(child, level + 1, from, to, out);
        }
    }

    private List<JsonValue> indexAll(JsonPath.IndexAll node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : eval(node.input, current)) {
            if (item.isArray()) {
                out.addAll(item.elements());
            } else if (strict) {
                throw new MemgresException(
                        "jsonpath wildcard array accessor can only be applied to an array", "22039");
            } else {
                out.add(item);                            // lax reads a lone item as a lone array
            }
        }
        return out;
    }

    private List<JsonValue> index(JsonPath.Index node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : eval(node.input, current)) {
            JsonValue array = item;
            if (!array.isArray()) {
                if (strict) {
                    throw new MemgresException(
                            "jsonpath array accessor can only be applied to an array", "22039");
                }
                array = JsonValue.array(Collections.singletonList(item));
            }
            int size = array.size();
            int outerLast = lastSubscript;
            lastSubscript = size - 1;
            try {
                for (int i = 0; i < node.from.size(); i++) {
                    int low = subscript(node.from.get(i), current);
                    JsonPath.Node upper = node.to.get(i);
                    int high = upper == null ? low : subscript(upper, current);
                    if (strict && (low > high || high >= size || low < 0)) {
                        throw new MemgresException("jsonpath array subscript is out of bounds",
                                "22033");
                    }
                    for (int k = Math.max(low, 0); k <= Math.min(high, size - 1); k++) {
                        out.add(array.at(k));
                    }
                }
            } finally {
                lastSubscript = outerLast;
            }
        }
        return out;
    }

    /** A subscript is an expression, and has to come out as one number that is used truncated. */
    private int subscript(JsonPath.Node node, JsonValue current) {
        List<JsonValue> items = unwrap(eval(node, current));
        if (items.size() != 1 || items.get(0).kind() != JsonValue.NUMBER) {
            throw new MemgresException("jsonpath array subscript is not a single numeric value",
                    "22033");
        }
        BigDecimal value = items.get(0).asNumber().setScale(0, RoundingMode.DOWN);
        if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0
                || value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
            throw new MemgresException("jsonpath array subscript is out of integer range", "22033");
        }
        return value.intValue();
    }

    private List<JsonValue> filter(JsonPath.Filter node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : unwrap(eval(node.input, current))) {
            if (Boolean.TRUE.equals(predicate(node.predicate, item))) out.add(item);
        }
        return out;
    }

    /**
     * In lax mode an array standing where a single item is wanted is opened, once. Nothing else
     * about the sequence changes, so a step that produced several items still produces several.
     */
    private List<JsonValue> unwrap(List<JsonValue> items) {
        if (strict) return items;
        boolean any = false;
        for (JsonValue item : items) {
            if (item.isArray()) { any = true; break; }
        }
        if (!any) return items;
        List<JsonValue> out = new ArrayList<JsonValue>(items.size());
        for (JsonValue item : items) {
            if (item.isArray()) out.addAll(item.elements());
            else out.add(item);
        }
        return out;
    }

    // ----------------------------------------------------------------- arithmetic

    private List<JsonValue> binary(JsonPath.Binary node, JsonValue current) {
        BigDecimal left = operand(node.left, current, node.op, "left");
        BigDecimal right = operand(node.right, current, node.op, "right");
        BigDecimal result;
        switch (node.op) {
            case '+': result = left.add(right); break;
            case '-': result = left.subtract(right); break;
            case '*': result = left.multiply(right); break;
            case '/':
                if (right.signum() == 0) {
                    throw new MemgresException("division by zero", "22012");
                }
                result = divide(left, right);
                break;
            default:
                if (right.signum() == 0) {
                    throw new MemgresException("division by zero", "22012");
                }
                result = left.remainder(right);
                break;
        }
        return Collections.singletonList(JsonValue.number(result));
    }

    /** Each side of a binary operator has to be one number: two of them is as wrong as none. */
    private BigDecimal operand(JsonPath.Node node, JsonValue current, char op, String side) {
        List<JsonValue> items = unwrap(eval(node, current));
        if (items.size() != 1 || items.get(0).kind() != JsonValue.NUMBER) {
            throw new MemgresException("" + side + " operand of jsonpath operator " + op
                    + " is not a single numeric value", "22038");
        }
        return items.get(0).asNumber();
    }

    /** Unary minus, unlike the binary operators, is applied to each item of the sequence. */
    private List<JsonValue> unary(JsonPath.Unary node, JsonValue current) {
        List<JsonValue> out = new ArrayList<JsonValue>();
        for (JsonValue item : unwrap(eval(node.operand, current))) {
            if (item.kind() != JsonValue.NUMBER) {
                throw new MemgresException("operand of unary jsonpath operator " + node.op
                        + " is not a numeric value", "2203B");
            }
            out.add(node.op == '-' ? JsonValue.number(item.asNumber().negate()) : item);
        }
        return out;
    }

    /**
     * Division carried to the scale PostgreSQL's numeric division reaches: sixteen significant
     * digits, rounded out to whole four-digit groups, and never fewer digits than either operand
     * already had. So {@code 1/3} answers with twenty places and {@code 100/3} with sixteen.
     */
    private static BigDecimal divide(BigDecimal a, BigDecimal b) {
        int qweight = nbaseWeight(a) - nbaseWeight(b);
        // A quotient whose leading group is no larger than the divisor's may not have reached the
        // next group, so it is weighed one group lower and given the four places that buys: 4/4
        // answers with twenty of them and 4/2 with sixteen.
        if (leadingGroup(a).compareTo(leadingGroup(b)) <= 0) qweight--;
        int rscale = Math.max(MIN_SIG_DIGITS - qweight * NBASE_DIGITS,
                Math.max(a.scale(), Math.max(b.scale(), 0)));
        return a.divide(b, rscale, RoundingMode.HALF_UP);
    }

    private static int nbaseWeight(BigDecimal v) {
        if (v.signum() == 0) return 0;
        int intDigits = v.precision() - v.scale();
        return Math.floorDiv(intDigits - 1, NBASE_DIGITS);
    }

    private static BigInteger leadingGroup(BigDecimal v) {
        if (v.signum() == 0) return BigInteger.ZERO;
        return v.abs().movePointLeft(nbaseWeight(v) * NBASE_DIGITS).toBigInteger();
    }

    // ----------------------------------------------------------------- predicates

    /** @return true, false, or null for unknown */
    private Boolean predicate(JsonPath.Node node, JsonValue current) {
        if (node instanceof JsonPath.Logic) {
            JsonPath.Logic logic = (JsonPath.Logic) node;
            Boolean left = predicate(logic.left, current);
            if (logic.and) {
                if (Boolean.FALSE.equals(left)) return Boolean.FALSE;
                Boolean right = predicate(logic.right, current);
                if (Boolean.FALSE.equals(right)) return Boolean.FALSE;
                return left == null || right == null ? null : Boolean.TRUE;
            }
            if (Boolean.TRUE.equals(left)) return Boolean.TRUE;
            Boolean right = predicate(logic.right, current);
            if (Boolean.TRUE.equals(right)) return Boolean.TRUE;
            return left == null || right == null ? null : Boolean.FALSE;
        }
        if (node instanceof JsonPath.Not) {
            Boolean arg = predicate(((JsonPath.Not) node).arg, current);
            return arg == null ? null : !arg;
        }
        if (node instanceof JsonPath.IsUnknown) {
            return predicate(((JsonPath.IsUnknown) node).arg, current) == null;
        }
        if (node instanceof JsonPath.Exists) {
            try {
                return !eval(((JsonPath.Exists) node).arg, current).isEmpty();
            } catch (MemgresException e) {
                return unknownOrRaise(e);
            }
        }
        if (node instanceof JsonPath.Compare) return compare((JsonPath.Compare) node, current);
        if (node instanceof JsonPath.StartsWith) {
            return startsWith((JsonPath.StartsWith) node, current);
        }
        if (node instanceof JsonPath.LikeRegex) return likeRegex((JsonPath.LikeRegex) node, current);
        // An expression standing where a predicate is wanted has to be a single boolean.
        List<JsonValue> items;
        try {
            items = eval(node, current);
        } catch (MemgresException e) {
            return unknownOrRaise(e);
        }
        if (items.size() != 1 || items.get(0).kind() != JsonValue.BOOLEAN) {
            throw new MemgresException("single boolean result is expected", "22038");
        }
        return items.get(0).asBoolean();
    }

    /**
     * A question about a document that the document has no answer to is unknown, not an error.
     * Only the errors that are about the document work that way; a path naming a variable that was
     * never passed is a mistake in the path itself and is raised wherever it stands.
     */
    private Boolean unknownOrRaise(MemgresException e) {
        if (JsonFunctions.isSuppressible(e)) return null;
        throw e;
    }

    private Boolean compare(JsonPath.Compare node, JsonValue current) {
        List<JsonValue> lefts;
        List<JsonValue> rights;
        try {
            lefts = unwrap(eval(node.left, current));
            rights = unwrap(eval(node.right, current));
        } catch (MemgresException e) {
            return unknownOrRaise(e);
        }
        boolean unknown = false;
        for (JsonValue left : lefts) {
            for (JsonValue right : rights) {
                Boolean pair = comparePair(node.op, left, right);
                if (pair == null) unknown = true;
                else if (pair) return Boolean.TRUE;
            }
        }
        return unknown ? null : Boolean.FALSE;
    }

    /**
     * Two items compare only where they are the same kind of thing. A number and a string are not
     * unequal, they are incomparable, and the answer is unknown. The one exception is null, which
     * every value is unequal to and none is ordered against.
     */
    private Boolean comparePair(String op, JsonValue left, JsonValue right) {
        if (left.kind() != right.kind()) {
            if (left.isNull() || right.isNull()) {
                return op.equals("!=") ? Boolean.TRUE : Boolean.FALSE;
            }
            return null;
        }
        int cmp;
        switch (left.kind()) {
            case JsonValue.NULL: cmp = 0; break;
            case JsonValue.BOOLEAN:
                cmp = (left.asBoolean() ? 1 : 0) - (right.asBoolean() ? 1 : 0);
                break;
            case JsonValue.NUMBER: cmp = left.asNumber().compareTo(right.asNumber()); break;
            case JsonValue.STRING: cmp = left.asString().compareTo(right.asString()); break;
            case JsonValue.DATETIME:
                Integer moments = JsonPathDatetime.compare(left.asDatetime(), right.asDatetime(),
                        tzAllowed);
                if (moments == null) return null;
                cmp = moments;
                break;
            default: return null;                        // arrays and objects have no order here
        }
        if (op.equals("==")) return cmp == 0;
        if (op.equals("!=")) return cmp != 0;
        if (op.equals("<")) return cmp < 0;
        if (op.equals("<=")) return cmp <= 0;
        if (op.equals(">")) return cmp > 0;
        return cmp >= 0;
    }

    private Boolean startsWith(JsonPath.StartsWith node, JsonValue current) {
        List<JsonValue> lefts;
        List<JsonValue> rights;
        try {
            lefts = unwrap(eval(node.left, current));
            rights = eval(node.right, current);
        } catch (MemgresException e) {
            return unknownOrRaise(e);
        }
        if (rights.size() != 1 || rights.get(0).kind() != JsonValue.STRING) return null;
        String prefix = rights.get(0).asString();
        boolean unknown = false;
        for (JsonValue left : lefts) {
            if (left.kind() != JsonValue.STRING) unknown = true;
            else if (left.asString().startsWith(prefix)) return Boolean.TRUE;
        }
        return unknown ? null : Boolean.FALSE;
    }

    private Boolean likeRegex(JsonPath.LikeRegex node, JsonValue current) {
        List<JsonValue> lefts;
        try {
            lefts = unwrap(eval(node.left, current));
        } catch (MemgresException e) {
            return unknownOrRaise(e);
        }
        boolean unknown = false;
        for (JsonValue left : lefts) {
            if (left.kind() != JsonValue.STRING) unknown = true;
            else if (node.pattern.matcher(left.asString()).find()) return Boolean.TRUE;
        }
        return unknown ? null : Boolean.FALSE;
    }

    // --------------------------------------------------------------- item methods

    private List<JsonValue> method(JsonPath.Method node, JsonValue current) {
        List<JsonValue> in = eval(node.input, current);
        List<JsonValue> out = new ArrayList<JsonValue>();
        // .type() and .size() describe the item they are handed, so an array is the answer rather
        // than something to look inside; every other method wants a single value and unwraps.
        if (node.name.equals("type")) {
            for (JsonValue item : in) out.add(JsonValue.string(item.typeName()));
            return out;
        }
        if (node.name.equals("size")) {
            for (JsonValue item : in) {
                if (item.isArray()) {
                    out.add(JsonValue.number(BigDecimal.valueOf(item.size())));
                } else if (strict) {
                    throw new MemgresException(
                            "jsonpath item method .size() can only be applied to an array", "22039");
                } else {
                    out.add(JsonValue.number(BigDecimal.ONE));
                }
            }
            return out;
        }
        List<BigDecimal> args = methodArgs(node, current);
        String template = node.name.equals("datetime") && !node.args.isEmpty()
                ? templateOf(node, current) : null;
        for (JsonValue item : unwrap(in)) apply(node.name, args, template, item, out);
        return out;
    }

    /** The to_char-style template a {@code .datetime(...)} was written with. */
    private String templateOf(JsonPath.Method node, JsonValue current) {
        List<JsonValue> items = eval(node.args.get(0), current);
        if (items.size() != 1 || items.get(0).kind() != JsonValue.STRING) {
            throw new MemgresException(
                    "invalid jsonpath item method .datetime() argument", "22036");
        }
        return items.get(0).asString();
    }

    /**
     * The precision and scale a method was written with. {@code .datetime()} takes a template
     * rather than numbers and is read straight off the parse tree instead.
     */
    private List<BigDecimal> methodArgs(JsonPath.Method node, JsonValue current) {
        if (node.args.isEmpty() || node.name.equals("datetime")) {
            return Collections.emptyList();
        }
        List<BigDecimal> args = new ArrayList<BigDecimal>(node.args.size());
        for (JsonPath.Node arg : node.args) {
            List<JsonValue> items = eval(arg, current);
            if (items.size() != 1 || items.get(0).kind() != JsonValue.NUMBER) {
                throw new MemgresException("invalid jsonpath item method ." + node.name
                        + "() argument", "22036");
            }
            args.add(items.get(0).asNumber());
        }
        return args;
    }

    private void apply(String name, List<BigDecimal> args, String template, JsonValue item,
                       List<JsonValue> out) {
        if (DATETIME_METHODS.contains(name)) {
            out.add(JsonValue.datetime(datetime(name, args, template, item)));
            return;
        }
        if (name.equals("keyvalue")) {
            keyValue(item, out);
            return;
        }
        if (name.equals("string")) {
            out.add(JsonValue.string(asText(item)));
            return;
        }
        if (name.equals("boolean")) {
            out.add(JsonValue.bool(asBoolean(item)));
            return;
        }
        if (name.equals("abs") || name.equals("floor") || name.equals("ceiling")) {
            BigDecimal value = numeric(name, item);
            if (name.equals("abs")) out.add(JsonValue.number(value.abs()));
            else if (name.equals("floor")) {
                out.add(JsonValue.number(value.setScale(0, RoundingMode.FLOOR)));
            } else out.add(JsonValue.number(value.setScale(0, RoundingMode.CEILING)));
            return;
        }
        out.add(JsonValue.number(converted(name, args, item)));
    }

    /**
     * Each pair of an object becomes an object of its own.
     *
     * <p>The {@code id} names the object the pair came out of, so that pairs gathered from several
     * objects at once can still be told apart and pairs of one object grouped together. PostgreSQL
     * numbers by the offset of the object inside the stored jsonb, which cannot be reproduced
     * without its binary layout; what is reproduced is what the number is for -- the document
     * itself is 0, and every object below it has a number of its own, the same number whichever
     * path reached it.
     */
    private void keyValue(JsonValue item, List<JsonValue> out) {
        if (!item.isObject()) {
            throw new MemgresException(
                    "jsonpath item method .keyvalue() can only be applied to an object", "2203C");
        }
        JsonValue id = JsonValue.number(BigDecimal.valueOf(objectId(item)));
        List<String> keys = Arrays.asList("id", "key", "value");
        for (int i = 0; i < item.size(); i++) {
            out.add(JsonValue.object(keys, Arrays.asList(id,
                    JsonValue.string(item.keyAt(i)), item.at(i))));
        }
    }

    /** Which object of the document this is, counting the document itself as the first. */
    private int objectId(JsonValue object) {
        if (objectIds == null) {
            objectIds = new java.util.IdentityHashMap<JsonValue, Integer>();
            numberValues(root, new int[1]);
        }
        Integer id = objectIds.get(object);
        // An object the document does not hold was built during the evaluation and stands alone,
        // so it is the only object of itself and the document's own number will do.
        return id == null ? 0 : id;
    }

    private void numberValues(JsonValue value, int[] next) {
        int here = next[0]++;
        if (value.isObject()) {
            objectIds.put(value, here);
            for (int i = 0; i < value.size(); i++) numberValues(value.at(i), next);
        } else if (value.isArray()) {
            for (JsonValue element : value.elements()) numberValues(element, next);
        }
    }

    private static BigDecimal numeric(String name, JsonValue item) {
        if (item.kind() != JsonValue.NUMBER) {
            throw new MemgresException("jsonpath item method ." + name
                    + "() can only be applied to a numeric value", "22036");
        }
        return item.asNumber();
    }

    private static String asText(JsonValue item) {
        switch (item.kind()) {
            case JsonValue.STRING: return item.asString();
            case JsonValue.NUMBER: return item.numberText();
            case JsonValue.BOOLEAN: return item.asBoolean() ? "true" : "false";
            case JsonValue.DATETIME: return item.asString();
            default:
                throw new MemgresException("jsonpath item method .string() can only be applied to"
                        + " a boolean, string, numeric, or datetime value", "22036");
        }
    }

    private static boolean asBoolean(JsonValue item) {
        if (item.kind() == JsonValue.BOOLEAN) return item.asBoolean();
        if (item.kind() == JsonValue.NUMBER) {
            BigDecimal value = item.asNumber();
            if (value.stripTrailingZeros().scale() > 0) throw invalidArgument("boolean", item);
            return value.signum() != 0;
        }
        if (item.kind() != JsonValue.STRING) {
            throw new MemgresException("jsonpath item method .boolean() can only be applied to a"
                    + " boolean, string, or numeric value", "22036");
        }
        String text = item.asString().trim().toLowerCase(java.util.Locale.ROOT);
        if (text.equals("t") || text.equals("true") || text.equals("y") || text.equals("yes")
                || text.equals("on") || text.equals("1")) {
            return true;
        }
        if (text.equals("f") || text.equals("false") || text.equals("n") || text.equals("no")
                || text.equals("off") || text.equals("0")) {
            return false;
        }
        throw invalidArgument("boolean", item);
    }

    /**
     * The numeric conversions. A number is converted the way a cast would convert it -- rounded
     * to fit -- while a string is read as the target type is written, so {@code "1.7"} is a
     * bigint's idea of nonsense even though {@code 1.7} is a bigint's idea of two.
     */
    private BigDecimal converted(String name, List<BigDecimal> args, JsonValue item) {
        BigDecimal value;
        if (item.kind() == JsonValue.NUMBER) {
            value = item.asNumber();
            if (isIntegerTarget(name)) {
                value = value.setScale(0, RoundingMode.HALF_UP);
            }
        } else if (item.kind() == JsonValue.STRING) {
            try {
                value = new BigDecimal(item.asString().trim());
            } catch (NumberFormatException e) {
                // "inf" and "nan" are values float8 and numeric can be written as but that a
                // jsonpath has nowhere to put, so they are refused for what they are rather than
                // reported as text that did not read as a number.
                if (isNotFinite(item.asString()) && !isIntegerTarget(name)) {
                    throw new MemgresException("NaN or Infinity is not allowed for jsonpath item"
                            + " method ." + name + "()", "22036");
                }
                throw invalidArgument(name, item);
            }
            if (isIntegerTarget(name) && value.stripTrailingZeros().scale() > 0) {
                throw invalidArgument(name, item);
            }
            if (isIntegerTarget(name)) value = value.setScale(0, RoundingMode.UNNECESSARY);
        } else {
            throw new MemgresException("jsonpath item method ." + name + "() can only be applied"
                    + " to a string or numeric value", "22036");
        }
        if (name.equals("integer")) return inRange(name, item, value, Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (name.equals("bigint")) {
            return inRange(name, item, value, Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (name.equals("double")) return asDouble(item, value);
        if (name.equals("decimal")) return withPrecision(args, item, value);
        return value;                                    // .number() keeps the value as it is
    }

    private static boolean isNotFinite(String text) {
        String t = text.trim().toLowerCase(java.util.Locale.ROOT);
        if (t.startsWith("+") || t.startsWith("-")) t = t.substring(1);
        return t.equals("inf") || t.equals("infinity") || t.equals("nan");
    }

    private static boolean isIntegerTarget(String name) {
        return name.equals("integer") || name.equals("bigint");
    }

    private static BigDecimal inRange(String name, JsonValue item, BigDecimal value,
                                      long low, long high) {
        if (value.compareTo(BigDecimal.valueOf(low)) < 0
                || value.compareTo(BigDecimal.valueOf(high)) > 0) {
            throw invalidArgument(name, item);
        }
        return value;
    }

    /**
     * A double is the nearest one, and there is no double that far out for a value that big.
     * The number that comes back is the one the double prints as rather than the exact binary
     * value it holds, which is the difference between {@code 1e300} and three hundred digits of
     * accident.
     */
    private static BigDecimal asDouble(JsonValue item, BigDecimal value) {
        double d = value.doubleValue();
        if (Double.isNaN(d) || Double.isInfinite(d)) throw invalidArgument("double", item);
        return new BigDecimal(Double.toString(d)).stripTrailingZeros();
    }

    private static BigDecimal withPrecision(List<BigDecimal> args, JsonValue item,
                                            BigDecimal value) {
        if (args.isEmpty()) return value;
        int precision = args.get(0).intValue();
        int scale = args.size() > 1 ? args.get(1).intValue() : 0;
        BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);
        if (scaled.precision() - scaled.scale() > precision - scale) {
            throw invalidArgument("decimal", item);
        }
        return scaled;
    }

    private static MemgresException invalidArgument(String name, JsonValue item) {
        String type = name.equals("double") ? "double precision"
                : name.equals("decimal") || name.equals("number") ? "numeric" : name;
        return new MemgresException("argument \"" + textOf(item) + "\" of jsonpath item method ."
                + name + "() is invalid for type " + type, "22036");
    }

    private static String textOf(JsonValue item) {
        return item.kind() == JsonValue.STRING ? item.asString() : item.toString();
    }

    private JsonPathDatetime datetime(String name, List<BigDecimal> args, String template,
                                      JsonValue item) {
        if (item.kind() != JsonValue.STRING) {
            throw new MemgresException("jsonpath item method ." + name
                    + "() can only be applied to a string", "22031");
        }
        String text = item.asString();
        JsonPathDatetime parsed = template == null ? JsonPathDatetime.parse(text)
                : JsonPathDatetime.parse(text, template);
        if (parsed == null) unrecognized(name, text);
        if (!name.equals("datetime")) {
            JsonPathDatetime cast = parsed.cast(targetType(name), tzAllowed);
            if (cast == null) unrecognized(name, text);
            parsed = cast;
        }
        if (!args.isEmpty()) parsed = parsed.rounded(args.get(0).intValue());
        return parsed;
    }

    private static void unrecognized(String name, String text) {
        MemgresException e = new MemgresException(
                name + " format is not recognized: \"" + text + "\"", "22031");
        // The typed methods name the shape they wanted; .datetime() takes whatever it is given,
        // so the only thing left to suggest is saying what shape that was.
        if (name.equals("datetime")) {
            e.setHint("Use a datetime template argument to specify the input data format.");
        }
        throw e;
    }

    private static int targetType(String name) {
        if (name.equals("date")) return JsonPathDatetime.DATE;
        if (name.equals("time")) return JsonPathDatetime.TIME;
        if (name.equals("time_tz")) return JsonPathDatetime.TIME_TZ;
        if (name.equals("timestamp")) return JsonPathDatetime.TIMESTAMP;
        return JsonPathDatetime.TIMESTAMP_TZ;
    }
}
