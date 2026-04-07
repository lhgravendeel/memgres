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

    Object evalArraySlice(ArraySliceExpr slice, RowContext ctx) {
        Object arrVal = executor.evalExpr(slice.array(), ctx);
        if (arrVal == null) return null;
        List<?> elements;
        int lowerBound = 1;
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
        } else if (arrVal instanceof String && ((String) arrVal).startsWith("{") && ((String) arrVal).endsWith("}")) {
            String s = (String) arrVal;
            String inner = s.substring(1, s.length() - 1);
            if (inner.isEmpty()) elements = java.util.Collections.emptyList();
            else {
                elements = java.util.Arrays.asList(inner.split(",", -1));
            }
        } else {
            return arrVal; // not an array, return as-is
        }

        int size = elements.size();
        // PG 1-based indexing; lower is inclusive, upper is inclusive
        int lo = slice.lower() != null ? executor.toInt(executor.evalExpr(slice.lower(), ctx)) : lowerBound;
        int hi = slice.upper() != null ? executor.toInt(executor.evalExpr(slice.upper(), ctx)) : lowerBound + size - 1;

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
        return formatArrayForOutput(sub);
    }

    String formatArrayForOutput(List<?> elements) {
        if (elements.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sb.append(",");
            Object e = elements.get(i);
            if (e == null) sb.append("NULL");
            else sb.append(e);
        }
        sb.append("}");
        return sb.toString();
    }

    List<Object> parsePostgresArrayLiteral(String s) {
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
            } else {
                // Unquoted element
                int start = i;
                while (i < inner.length() && inner.charAt(i) != ',') i++;
                String elem = inner.substring(start, i).trim();
                if (elem.equalsIgnoreCase("NULL")) {
                    result.add(null);
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
                        try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02");
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
