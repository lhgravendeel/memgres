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

    private static String formatTimestampForRange(Object val) {
        if (val instanceof java.time.OffsetDateTime) {
            java.time.OffsetDateTime odt = (java.time.OffsetDateTime) val;
            java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssxxx");
            String formatted = odt.format(fmt);
            if (formatted.endsWith(":00")) {
                formatted = formatted.substring(0, formatted.length() - 3);
            }
            return formatted;
        }
        if (val instanceof java.time.LocalDateTime) {
            java.time.LocalDateTime ldt = (java.time.LocalDateTime) val;
            return ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        return val.toString();
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "int4range":
            case "int8range": {
                Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                String bounds = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx).toString() : "[)";
                Integer lo = loObj == null ? null : executor.toInt(loObj);
                Integer hi = hiObj == null ? null : executor.toInt(hiObj);
                return RangeOperations.int4rangeNullable(lo, hi, bounds).toString();
            }
            case "daterange": {
                Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                String bounds = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx).toString() : "[)";
                boolean li = bounds.charAt(0) == '[';
                boolean ui = bounds.charAt(1) == ']';
                String loStr = loObj == null ? "" : loObj.toString();
                String hiStr = hiObj == null ? "" : hiObj.toString();
                String lBracket = loObj == null ? "(" : (li ? "[" : "(");
                String rBracket = hiObj == null ? ")" : (ui ? "]" : ")");
                return lBracket + loStr + "," + hiStr + rBracket;
            }
            case "tsrange":
            case "tstzrange": {
                Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                String bounds = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx).toString() : "[)";
                boolean li = bounds.charAt(0) == '[';
                boolean ui = bounds.charAt(1) == ']';
                String loStr = loObj == null ? "" : formatTimestampForRange(loObj);
                String hiStr = hiObj == null ? "" : formatTimestampForRange(hiObj);
                String lBracket = loObj == null ? "(" : (li ? "[" : "(");
                String rBracket = hiObj == null ? ")" : (ui ? "]" : ")");
                return lBracket + "\"" + loStr + "\",\"" + hiStr + "\"" + rBracket;
            }
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
                    if (rv == null) continue;
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
                    intRanges.sort((a, b) -> Long.compare(a.effectiveLower(), b.effectiveLower()));
                    List<RangeOperations.PgRange> merged = new ArrayList<>();
                    merged.add(intRanges.get(0));
                    for (int i = 1; i < intRanges.size(); i++) {
                        RangeOperations.PgRange last = merged.get(merged.size() - 1);
                        RangeOperations.PgRange curr = intRanges.get(i);
                        if (last.effectiveUpper() >= curr.effectiveLower()) {
                            merged.set(merged.size() - 1, RangeOperations.merge(last, curr));
                        } else {
                            merged.add(curr);
                        }
                    }
                    StringBuilder sb2 = new StringBuilder("{");
                    for (int i = 0; i < merged.size(); i++) {
                        if (i > 0) sb2.append(",");
                        sb2.append(merged.get(i));
                    }
                    sb2.append("}");
                    return sb2.toString();
                }
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < rawRanges.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(rawRanges.get(i));
                }
                sb.append("}");
                return sb.toString();
            }
            case "numrange": {
                if (fn.args().size() < 2) return null;
                Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                String bounds = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx).toString() : "[)";
                boolean li = bounds.charAt(0) == '[';
                boolean ui = bounds.charAt(1) == ']';
                String loStr = loObj == null ? "" : loObj.toString();
                String hiStr = hiObj == null ? "" : hiObj.toString();
                String lBracket = loObj == null ? "(" : (li ? "[" : "(");
                String rBracket = hiObj == null ? ")" : (ui ? "]" : ")");
                return lBracket + loStr + "," + hiStr + rBracket;
            }
            default: {
                // Check for user-defined range type constructors
                String subtype = executor.database.getRangeTypes().get(name);
                if (subtype != null) {
                    // User-defined range type constructor: treat like int4range
                    Object loObj = executor.evalExpr(fn.args().get(0), ctx);
                    Object hiObj = executor.evalExpr(fn.args().get(1), ctx);
                    String bounds = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx).toString() : "[)";
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
