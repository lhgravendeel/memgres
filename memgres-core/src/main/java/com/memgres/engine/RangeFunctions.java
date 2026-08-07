package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import java.util.*;

/**
 * Range function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class RangeFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    RangeFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    private static String getRangeTypeName(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = fn.name().toLowerCase();
            switch (name) {
                case "int4range":
                case "int8range":
                case "numrange":
                case "daterange":
                case "tsrange":
                case "tstzrange":
                    return name;
                default:
                    return null;
            }
        }
        return null;
    }

    private static boolean isDecimalRange(String s) {
        if (s == null || s.length() < 3) return false;
        String inner = s.substring(1, s.length() - 1);
        String[] parts = inner.split(",", 2);
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty() && t.contains(".")) return true;
        }
        return false;
    }

    /**
     * Build a range from the two bound values a constructor was given, reading each as a value of
     * {@code rangeType}'s element type. Going through the range's own text form is what makes
     * {@code tsrange(date, date)} come back as the pair of timestamps it actually holds.
     */
    /**
     * The bound flags a range constructor was given, or {@code "[)"} when it was given none.
     * A NULL is not a default: PostgreSQL refuses it, because which bounds were meant is exactly
     * what the argument was there to say.
     */
    private String boundFlags(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().size() <= 2) return "[)";
        Object flags = executor.evalExpr(fn.args().get(2), ctx);
        if (flags == null) {
            throw new MemgresException(
                    "range constructor flags argument must not be null", "22000");
        }
        return flags.toString();
    }

    private String buildRange(String rangeType, FunctionCallExpr fn, RowContext ctx) {
        String cast = castedRange(rangeType, fn, ctx);
        if (cast != NOT_A_CAST) return cast;
        Object loObj = executor.evalExpr(fn.args().get(0), ctx);
        Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
        String bounds = boundFlags(fn, ctx);
        if (bounds.length() != 2
                || (bounds.charAt(0) != '[' && bounds.charAt(0) != '(')
                || (bounds.charAt(1) != ']' && bounds.charAt(1) != ')')) {
            throw new MemgresException(
                    "range bound flags must be one of \"[]\", \"[)\", \"(]\", or \"()\"", "22P02");
        }
        String lo = loObj == null ? "" : quoteForRange(loObj);
        String hi = hiObj == null ? "" : quoteForRange(hiObj);
        String text = bounds.charAt(0) + lo + "," + hi + bounds.charAt(1);
        return RangeOperations.parse(text, rangeType).toString();
    }

    /** Told apart from a null result, which is what a cast of NULL answers. */
    private static final String NOT_A_CAST = new String("not a cast");

    /**
     * A range type name written like a call of one argument, which PostgreSQL reads as a cast:
     * {@code daterange('[2020-01-01,2020-02-01)')} is CAST(… AS daterange) and
     * {@code daterange(NULL)} is a null range. Reading a second bound off an argument list that
     * has only one was an index off the end, and the client was told XX000.
     */
    private String castedRange(String rangeType, FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().size() != 1) return NOT_A_CAST;
        Object only = executor.evalExpr(fn.args().get(0), ctx);
        if (only == null) return null;
        return RangeOperations.parse(only.toString(), rangeType).toString();
    }

    /** A bound written back into a range literal has to survive being read out of it again. */
    private static String quoteForRange(Object val) {
        String s;
        if (val instanceof java.time.LocalDateTime) {
            s = RangeOperations.formatTimestamp((java.time.LocalDateTime) val);
        } else if (val instanceof java.time.OffsetDateTime) {
            s = RangeOperations.formatTimestamptz((java.time.OffsetDateTime) val);
        } else {
            s = val.toString();
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /**
     * {@code 42883} when a range constructor is handed a bound of a wider type than the range is
     * built over. PostgreSQL declares {@code int4range(integer, integer)} and nothing else, and a
     * bigint argument matches no candidate at all -- it does not quietly narrow. Reading one as an
     * int instead is what let {@code int4range(1, 99999999999)} answer {@code [1,1215752191)}.
     *
     * <p>The rule fires only on the value classes that can only have come from a wider type: an
     * argument the engine hands over as an Integer or a Short is left alone, so nothing that
     * PostgreSQL resolves is refused here.
     */
    private static void rejectWiderBound(String rangeType, Object lo, Object hi) {
        String loName = widerBoundTypeName(lo);
        String hiName = widerBoundTypeName(hi);
        if (loName == null && hiName == null) return;
        throw new MemgresException("function " + rangeType + "("
                + (loName != null ? loName : "integer") + ", "
                + (hiName != null ? hiName : "integer") + ") does not exist"
                + "\n  Hint: No function matches the given name and argument types."
                + " You might need to add explicit type casts.", "42883");
    }

    /** The PG type name a bound value can only have come from, or null when int4 accepts it. */
    private static String widerBoundTypeName(Object value) {
        if (value instanceof Long) return "bigint";
        if (value instanceof java.math.BigInteger
                || value instanceof java.math.BigDecimal) return "numeric";
        if (value instanceof Float) return "real";
        if (value instanceof Double) return "double precision";
        return null;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "int4range": {
                String cast = castedRange("int4range", fn, ctx);
                if (cast != NOT_A_CAST) return cast;
                Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                rejectWiderBound("int4range", loObj, hiObj);
                String bounds = boundFlags(fn, ctx);
                Integer lo = loObj == null ? null : executor.toInt(loObj);
                Integer hi = hiObj == null ? null : executor.toInt(hiObj);
                return RangeOperations.int4rangeNullable(lo, hi, bounds).toString();
            }
            case "int8range":
                // int8range holds bigint bounds; reading them as int wrapped 99999999999 round to
                // 1215752191 and built a plausible-looking range over a bound nobody asked for.
                return buildRange(name, fn, ctx);
            case "daterange":
            case "tsrange":
            case "tstzrange":
                return buildRange(name, fn, ctx);
            case "lower_inc": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(s)) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                    if (ranges.isEmpty()) return false;
                    return ranges.get(0).lowerInclusive();
                }
                if (s.equalsIgnoreCase("empty")) return false;
                return s.charAt(0) == '[';
            }
            case "upper_inc": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(s)) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                    if (ranges.isEmpty()) return false;
                    return ranges.get(ranges.size() - 1).upperInclusive();
                }
                if (s.equalsIgnoreCase("empty")) return false;
                return s.charAt(s.length() - 1) == ']';
            }
            case "upper_inf": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(s)) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                    if (ranges.isEmpty()) return false;
                    RangeOperations.PgRange last = ranges.get(ranges.size() - 1);
                    return last.upper() == null;
                }
                if (s.equalsIgnoreCase("empty")) return false;
                RangeOperations.PgRange r = RangeOperations.parse(s);
                return r.upper() == null;
            }
            case "lower_inf": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(s)) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                    if (ranges.isEmpty()) return false;
                    return ranges.get(0).lower() == null;
                }
                if (s.equalsIgnoreCase("empty")) return false;
                RangeOperations.PgRange r = RangeOperations.parse(s);
                return r.lower() == null;
            }
            case "range_merge": {
                // range_merge(multirange) → single range spanning all sub-ranges
                if (fn.args().size() == 1) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg == null) return null;
                    String s = arg.toString().trim();
                    if (RangeOperations.isMultirangeOrEmpty(s)) {
                        List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                        if (ranges.isEmpty()) return "empty";
                        RangeOperations.PgRange result = ranges.get(0);
                        for (int i = 1; i < ranges.size(); i++) {
                            result = RangeOperations.merge(result, ranges.get(i));
                        }
                        return result.toString();
                    }
                }
                // range_merge(range, range) → smallest range containing both
                // Check for cross-type range arguments (e.g., int4range vs numrange)
                if (fn.args().size() == 2) {
                    String lt = getRangeTypeName(fn.args().get(0));
                    String rt = getRangeTypeName(fn.args().get(1));
                    if (lt != null && rt != null && !lt.equals(rt)) {
                        throw new MemgresException(
                            "function range_merge(" + lt + ", " + rt + ") does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                    }
                }
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                // Type check: arguments must be range strings
                if (!RangeOperations.isRangeString(a.toString()) || !RangeOperations.isRangeString(b.toString())) {
                    throw new MemgresException(
                        "function range_merge(text, text) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42804");
                }
                RangeOperations.PgRange ra = RangeOperations.parse(a.toString());
                RangeOperations.PgRange rb = RangeOperations.parse(b.toString());
                return RangeOperations.merge(ra, rb).toString();
            }
            case "isempty": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof String && RangeOperations.isMultirangeOrEmpty(((String) arg))) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(((String) arg));
                    // A multirange is empty if it has no sub-ranges or all are empty
                    if (ranges.isEmpty()) return true;
                    for (RangeOperations.PgRange r : ranges) {
                        if (!r.isEmpty()) return false;
                    }
                    return true;
                }
                if (arg instanceof String && RangeOperations.isRangeString(((String) arg))) {
                    String s = (String) arg;
                    return RangeOperations.parse(s).isEmpty();
                }
                return false;
            }
            case "unnest": {
                // unnest(multirange) → set of ranges (returns as a list for SRF processing)
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(s)) {
                    List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                    List<Object> result = new ArrayList<>();
                    for (RangeOperations.PgRange r : ranges) {
                        result.add(r.toString());
                    }
                    return result;
                }
                return NOT_HANDLED;
            }
            case "multirange": {
                // multirange(range) → wraps a single range into a multirange
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return "{}";
                String s = arg.toString().trim();
                if (s.equalsIgnoreCase("empty")) return "{}";
                if (RangeOperations.isRangeString(s)) return "{" + s + "}";
                return "{" + s + "}";
            }
            case "int4multirange":
            case "int8multirange":
            case "nummultirange":
            case "datemultirange":
            case "tsmultirange":
            case "tstzmultirange": {
                // Multirange constructors: merge overlapping/adjacent ranges and yield as {[...],[...]}
                for (Expression arg : fn.args()) {
                    if (arg instanceof Literal && ((Literal) arg).literalType() == Literal.LiteralType.STRING
                            && ((Literal) arg).value() != null) {
                        Literal lit = (Literal) arg;
                        String litVal = lit.value().trim();
                        if (RangeOperations.isRangeString(litVal)) {
                            throw new MemgresException("malformed multirange literal: \"" + litVal + "\"\n  Detail: Missing left brace.", "22P02");
                        }
                        throw new MemgresException("malformed multirange literal: \"" + litVal + "\"", "22P02");
                    }
                }
                if (fn.args().isEmpty()) return "{}";
                List<String> rawRanges = new ArrayList<>();
                List<RangeOperations.PgRange> intRanges = new ArrayList<>();
                boolean allInteger = true;
                for (Expression arg : fn.args()) {
                    Object rv = executor.evalExpr(arg, ctx);
                    // A multirange holds ranges, and NULL is not one. The single-argument form is
                    // the variadic array itself being NULL, which makes the call NULL and is
                    // answered before this runs; a NULL beside other ranges is a member the value
                    // cannot hold. Skipping it built a multirange out of the rest, so a NULL
                    // range silently disappeared from the value the caller got back.
                    if (rv == null) {
                        throw new MemgresException(
                                "multirange values cannot contain null members", "22004");
                    }
                    String rs = rv.toString().trim();
                    if (rs.equalsIgnoreCase("empty")) continue;
                    rawRanges.add(rs);
                    if (allInteger && RangeOperations.isRangeString(rs)) {
                        try {
                            RangeOperations.PgRange parsed = RangeOperations.parse(rs);
                            if (isDecimalRange(rs)) {
                                allInteger = false;
                            } else {
                                intRanges.add(parsed);
                            }
                        } catch (MemgresException e) {
                            allInteger = false;
                        }
                    } else {
                        allInteger = false;
                    }
                }
                if (rawRanges.isEmpty()) return "{}";
                if (allInteger && !intRanges.isEmpty()) {
                    return RangeOperations.formatMultirange(RangeOperations.mergeAndSort(intRanges));
                }
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < rawRanges.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(rawRanges.get(i));
                }
                sb.append("}");
                return sb.toString();
            }
            case "numrange":
                return buildRange(name, fn, ctx);
            default: {
                // Check for user-defined range type constructors
                String subtype = executor.database.getRangeSubtype(name);
                if (subtype != null) {
                    // User-defined range type constructor: treat like int4range
                    Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                    Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                    String bounds = boundFlags(fn, ctx);
                    // For integer subtypes, use canonical form
                    String st = subtype.toLowerCase();
                    if (st.equals("int4") || st.equals("integer") || st.equals("int") || st.equals("int8") || st.equals("bigint") || st.equals("smallint") || st.equals("int2")) {
                        Integer lo = loObj == null ? null : executor.toInt(loObj);
                        Integer hi = hiObj == null ? null : executor.toInt(hiObj);
                        return RangeOperations.int4rangeNullable(lo, hi, bounds).toString();
                    }
                    boolean li = bounds.charAt(0) == '[';
                    boolean ui = bounds.charAt(1) == ']';
                    String loStr = loObj == null ? "" : loObj.toString();
                    String hiStr = hiObj == null ? "" : hiObj.toString();
                    String lBracket = loObj == null ? "(" : (li ? "[" : "(");
                    String rBracket = hiObj == null ? ")" : (ui ? "]" : ")");
                    return lBracket + loStr + "," + hiStr + rBracket;
                }
                return NOT_HANDLED;
            }
        }
    }
}
