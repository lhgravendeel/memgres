package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Array operations, extracted from AstExecutor to reduce class size.
 */
class ArrayOperationHandler {
    private final AstExecutor executor;

    ArrayOperationHandler(AstExecutor executor) {
        this.executor = executor;
    }

    /** Read an array value in any of the shapes it may arrive in, or null if it is not one. */
    private List<Object> asElements(Object arrVal) {
        if (arrVal == null) return new ArrayList<Object>();
        if (arrVal instanceof List<?>) return new ArrayList<Object>((List<?>) arrVal);
        if (arrVal instanceof String) {
            String s = ((String) arrVal).trim();
            if (s.startsWith("{") && s.endsWith("}")) {
                return new ArrayList<Object>(FunctionEvaluator.parseSimplePgArray(s));
            }
        }
        return null;
    }

    /**
     * Assign one element of an array, 1-based. Writing past the end grows the array and leaves
     * NULLs in the gap, the way PG does.
     */
    Object assignElement(Object arrVal, int index, Object value) {
        List<Object> elements = asElements(arrVal);
        if (elements == null) return arrVal;
        if (index < 1) return formatArrayForOutput(elements);
        while (elements.size() < index) elements.add(null);
        elements.set(index - 1, value);
        return formatArrayForOutput(elements);
    }

    /**
     * Replace the elements between two 1-based bounds with the elements of {@code value}. A slice
     * that reaches past the end extends the array rather than being clipped to it.
     */
    Object assignSlice(Object arrVal, int lower, int upper, Object value) {
        List<Object> elements = asElements(arrVal);
        if (elements == null) return arrVal;
        List<Object> replacement = asElements(value);
        if (replacement == null) {
            replacement = new ArrayList<Object>();
            replacement.add(value);
        }
        if (lower < 1) lower = 1;
        if (upper < lower) return formatArrayForOutput(elements);
        while (elements.size() < upper) elements.add(null);
        for (int i = lower; i <= upper; i++) {
            int replIdx = i - lower;
            elements.set(i - 1, replIdx < replacement.size() ? replacement.get(replIdx) : null);
        }
        return formatArrayForOutput(elements);
    }

    Object evalArraySlice(ArraySliceExpr slice, RowContext ctx) {
        Object arrVal = executor.evalExpr(slice.array(), ctx);
        if (arrVal == null) return null;
        List<?> elements;
        int lowerBound = 1;
        // Multi-dim slicing: when the source is itself a slice result (chained [1:2][1:1])
        // and elements are lists, apply slice to each sub-array (column-wise)
        boolean isMultiDim = false;
        boolean isChainedSlice = slice.array() instanceof ArraySliceExpr;
        if (arrVal instanceof String && ((String) arrVal).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
            String s = (String) arrVal;
            // Custom lower-bound array: "[lb:ub]={...}"
            int eqIdx = s.indexOf('=');
            String boundsStr = s.substring(0, eqIdx);
            String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
            lowerBound = Integer.parseInt(parts[0].trim());
            String content = s.substring(eqIdx + 1);
            elements = FunctionEvaluator.parseSimplePgArray(content);
        } else if (arrVal instanceof List<?>) {
            elements = (List<?>) arrVal;
            isMultiDim = isChainedSlice && !elements.isEmpty() && elements.get(0) instanceof List<?>;
        } else if (arrVal instanceof String && ((String) arrVal).startsWith("{") && ((String) arrVal).endsWith("}")) {
            // Quote- and nesting-aware parse (commas inside quoted elements are not separators)
            elements = FunctionEvaluator.parseSimplePgArray((String) arrVal);
        } else {
            return arrVal; // not an array, return as-is
        }

        int size = elements.size();
        // PG 1-based indexing; lower is inclusive, upper is inclusive
        int lo = slice.lower() != null ? executor.toInt(executor.evalExpr(slice.lower(), ctx)) : lowerBound;
        int hi = slice.upper() != null ? executor.toInt(executor.evalExpr(slice.upper(), ctx)) : lowerBound + size - 1;

        // Multi-dimensional array: apply slice to each sub-array (column-wise slicing)
        if (isMultiDim) {
            List<Object> result = new ArrayList<>();
            for (Object elem : elements) {
                if (elem instanceof List<?>) {
                    List<?> subList = (List<?>) elem;
                    int subSize = subList.size();
                    int loIdx = lo - lowerBound;
                    int hiIdx = hi - lowerBound;
                    if (loIdx > hiIdx || hiIdx < 0 || loIdx >= subSize) {
                        result.add(new ArrayList<>());
                    } else {
                        int from = Math.max(0, loIdx);
                        int to = Math.min(subSize - 1, hiIdx);
                        result.add(new ArrayList<>(subList.subList(from, to + 1)));
                    }
                } else {
                    result.add(elem);
                }
            }
            return formatArrayForOutput(result);
        }

        // Convert to 0-based index
        int loIdx = lo - lowerBound;
        int hiIdx = hi - lowerBound;

        if (size == 0) {
            return null;
        }
        if (loIdx > hiIdx || hiIdx < 0 || loIdx >= size) {
            return "{}";
        }
        int from = Math.max(0, loIdx);
        int to = Math.min(size - 1, hiIdx);
        List<?> sub = new java.util.ArrayList<>(elements.subList(from, to + 1));

        // Multi-dimensional array: if elements are lists, return as List (not string)
        // so that chained slices can apply to each sub-array
        if (!sub.isEmpty() && sub.get(0) instanceof List<?>) {
            return sub;
        }
        return formatArrayForOutput(sub);
    }

    String formatArrayForOutput(List<?> elements) {
        if (elements.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sb.append(",");
            Object e = elements.get(i);
            if (e == null) sb.append("NULL");
            else if (e instanceof List<?>) sb.append(formatArrayForOutput((List<?>) e));
            else if (e instanceof String && needsArrayQuoting((String) e)) {
                sb.append("\"").append(((String) e).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
            } else sb.append(e);
        }
        sb.append("}");
        return sb.toString();
    }

    /** True if a string element must be double-quoted in PG array output syntax. */
    private static boolean needsArrayQuoting(String s) {
        if (s.isEmpty() || s.equalsIgnoreCase("NULL")) return true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' || Character.isWhitespace(c)) return true;
        }
        return false;
    }

    List<Object> parsePostgresArrayLiteral(String s) {
        return parsePostgresArrayLiteral(s, false);
    }

    /**
     * Parse a PG array literal like {a,"b,c",NULL}.
     *
     * @param rawStrings when true, unquoted elements keep their raw text (no eager
     *        Integer/Long/BigDecimal conversion) so a downstream cast to the target
     *        element type sees the original spelling (e.g. "01", "+5"). Unquoted
     *        NULL still becomes SQL null. When false, numeric-looking unquoted
     *        elements are typed, which the = ANY()/IN equality paths rely on.
     */
    List<Object> parsePostgresArrayLiteral(String s, boolean rawStrings) {
        String inner = s.substring(1, s.length() - 1).trim();
        if (inner.isEmpty()) return Cols.listOf();

        List<Object> result = new java.util.ArrayList<>();
        int i = 0;
        while (i < inner.length()) {
            if (inner.charAt(i) == '"') {
                // Quoted element
                i++; // skip opening quote
                StringBuilder sb = new StringBuilder();
                while (i < inner.length()) {
                    if (inner.charAt(i) == '\\' && i + 1 < inner.length()) {
                        sb.append(inner.charAt(i + 1));
                        i += 2;
                    } else if (inner.charAt(i) == '"') {
                        i++; // skip closing quote
                        break;
                    } else {
                        sb.append(inner.charAt(i));
                        i++;
                    }
                }
                result.add(sb.toString());
                // Skip comma
                while (i < inner.length() && (inner.charAt(i) == ',' || inner.charAt(i) == ' ')) i++;
            } else if (inner.charAt(i) == '{') {
                // Nested sub-array: find the matching close brace (quote-aware)
                int depth = 0;
                boolean inQ = false;
                int start = i;
                int j = i;
                for (; j < inner.length(); j++) {
                    char cc = inner.charAt(j);
                    if (inQ) {
                        if (cc == '\\') j++;
                        else if (cc == '"') inQ = false;
                    } else if (cc == '"') inQ = true;
                    else if (cc == '{') depth++;
                    else if (cc == '}') {
                        depth--;
                        if (depth == 0) break;
                    }
                }
                int end = Math.min(j, inner.length() - 1);
                result.add(parsePostgresArrayLiteral(inner.substring(start, end + 1), rawStrings));
                i = end + 1;
                while (i < inner.length() && (inner.charAt(i) == ',' || inner.charAt(i) == ' ')) i++;
            } else {
                // Unquoted element
                int start = i;
                while (i < inner.length() && inner.charAt(i) != ',') i++;
                String elem = inner.substring(start, i).trim();
                if (elem.equalsIgnoreCase("NULL")) {
                    result.add(null);
                } else if (rawStrings) {
                    result.add(elem);
                } else {
                    try {
                        if (elem.contains(".") || elem.contains("e") || elem.contains("E")) {
                            result.add(new java.math.BigDecimal(elem));
                        } else {
                            long lv = Long.parseLong(elem);
                            result.add(lv >= Integer.MIN_VALUE && lv <= Integer.MAX_VALUE ? (int) lv : lv);
                        }
                    } catch (NumberFormatException ex) {
                        result.add(elem);
                    }
                }
                if (i < inner.length() && inner.charAt(i) == ',') i++;
            }
        }
        return result;
    }

    Object evalArray(ArrayExpr arr, RowContext ctx) {
        List<Object> list = new ArrayList<>();
        for (Expression elem : arr.elements()) {
            list.add(executor.evalExpr(elem, ctx));
        }
        if (arr.isRow()) return new AstExecutor.PgRow(list);
        // Validate multi-dimensional array: all sub-arrays must have the same size
        if (!list.isEmpty() && list.get(0) instanceof List<?>) {
            int expectedSize = ((List<?>) list.get(0)).size();
            for (int i = 1; i < list.size(); i++) {
                if (list.get(i) instanceof List<?>) {
                    if (((List<?>) list.get(i)).size() != expectedSize) {
                        throw new MemgresException("multidimensional arrays must have array expressions with matching dimensions", "2202E");
                    }
                }
            }
        }
        // Validate element type homogeneity
        if (!arr.isRow() && !list.isEmpty() && !(list.get(0) instanceof List<?>)) {
            Object firstNonNull = null;
            for (Object v : list) { if (v != null) { firstNonNull = v; break; } }
            if (firstNonNull instanceof Number) {
                for (Object v : list) {
                    if (v != null && v instanceof String) {
                        String s = (String) v;
                        // Allow special float values (NaN, Infinity, -Infinity) alongside numeric literals
                        String lower = s.trim().toLowerCase();
                        if (!lower.equals("nan") && !lower.equals("infinity") && !lower.equals("-infinity")
                                && !lower.equals("+infinity") && !lower.equals("inf") && !lower.equals("-inf")) {
                            try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                                throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02");
                            }
                        }
                    }
                }
            }
        }
        return list;
    }

    Object evalArraySubquery(ArraySubqueryExpr asq, RowContext outerCtx) {
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(asq.subquery());
            List<Object> list = new ArrayList<>();
            for (Object[] row : result.getRows()) {
                list.add(row.length > 0 ? row[0] : null);
            }
            return list;
        } finally {
            if (outerCtx != null) executor.outerContextStack.pop();
        }
    }
}
