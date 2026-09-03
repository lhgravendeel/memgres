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

    /**
     * Concatenate two arrays. PostgreSQL joins two arrays of the same dimension end to end, but
     * appends an operand of one dimension less as a single element, so {@code {{1,2},{3,4}}} and
     * {@code {5,6}} make a three-row array rather than four loose values. An empty array has no
     * dimensions at all and simply leaves the other operand as it was.
     */
    /**
     * Two arrays joined end to end.
     *
     * <p>The answer starts where the left array started: PostgreSQL keeps the left operand's
     * lower bound and counts the joined elements from there, so {@code '[0:1]={1,2}' || '{3}'} is
     * an array of three subscripted from zero. Built with the ordinary bounds instead, the
     * subscripts every reader of the left array had been using moved by one.
     */
    static List<Object> concatArrays(List<?> left, List<?> right) {
        return keepingLowerBounds(left, concatElements(left, right));
    }

    /** The joined elements, subscripted from wherever the array they were added to began. */
    static List<Object> keepingLowerBounds(List<?> original, List<Object> joined) {
        if (!(original instanceof PgArray) || original.isEmpty()) return joined;
        int[] bounds = ((PgArray) original).lowerBounds();
        if (bounds == null || bounds.length == 0 || bounds[0] == 1) return joined;
        return PgArray.of(joined, bounds, ((PgArray) original).elementType());
    }

    private static List<Object> concatElements(List<?> left, List<?> right) {
        List<Object> merged = new ArrayList<Object>();
        if (left.isEmpty() || right.isEmpty()) {
            merged.addAll(left);
            merged.addAll(right);
            return merged;
        }
        int leftDims = dimensionsOf(left);
        int rightDims = dimensionsOf(right);
        if (leftDims == rightDims + 1) {
            merged.addAll(left);
            merged.add(new ArrayList<Object>(right));
        } else if (rightDims == leftDims + 1) {
            merged.add(new ArrayList<Object>(left));
            merged.addAll(right);
        } else {
            merged.addAll(left);
            merged.addAll(right);
        }
        return merged;
    }

    /** How many dimensions an array value has, counted down its first non-null element. */
    private static int dimensionsOf(List<?> array) {
        int dims = 1;
        Object first = firstNonNull(array);
        while (first instanceof List<?>) {
            dims++;
            first = firstNonNull((List<?>) first);
        }
        return dims;
    }

    private static Object firstNonNull(List<?> values) {
        for (Object v : values) {
            if (v != null) return v;
        }
        return null;
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

    /**
     * A slice {@code a[i:j]}. A bound that is not given is the array's own; a bound that is NULL
     * makes the whole slice NULL, because there is no range to take. A slice that falls outside
     * the array is the empty array rather than nothing at all.
     */
    Object evalArraySlice(ArraySliceExpr slice, RowContext ctx) {
        Object arrVal = executor.evalExpr(slice.array(), ctx);
        if (arrVal == null) return null;
        PgArray array = PgArray.from(arrVal);
        if (array == null) return arrVal;

        int lowerBound = array.lowerBound(1);
        int size = array.size();
        Integer lo = sliceBound(slice.lower(), ctx);
        Integer hi = sliceBound(slice.upper(), ctx);
        if (slice.lower() != null && lo == null) return null;
        if (slice.upper() != null && hi == null) return null;
        if (lo == null) lo = lowerBound;
        if (hi == null) hi = lowerBound + size - 1;

        int loIdx = lo - lowerBound;
        int hiIdx = hi - lowerBound;
        if (size == 0 || loIdx > hiIdx || hiIdx < 0 || loIdx >= size) {
            return PgArray.of(new ArrayList<Object>());
        }
        int from = Math.max(0, loIdx);
        int to = Math.min(size - 1, hiIdx);
        // A slice is an array in its own right, and PostgreSQL gives it the ordinary bounds
        // whatever bounds it was cut from.
        return PgArray.ofType(new ArrayList<Object>(array.subList(from, to + 1)),
                array.elementType());
    }

    /** One bound of a slice, or null when it is absent or evaluates to NULL. */
    private Integer sliceBound(Expression bound, RowContext ctx) {
        if (bound == null) return null;
        Object value = executor.evalExpr(bound, ctx);
        return value == null ? null : (Integer) executor.toInt(value);
    }

    String formatArrayForOutput(List<?> elements) {
        return TypeCoercion.formatPgArray(elements);
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
        // Each element stands where one value stands, so a subquery among them may have only one
        // column -- settled from its select list, whether or not the element is reached.
        ExprEvaluator.rejectWideSubqueryElements(arr.elements());
        List<Object> list = new ArrayList<>();
        for (Expression elem : arr.elements()) {
            Object value = executor.evalExpr(elem, ctx);
            // A whole row written into a row constructor contributes its fields, not itself:
            // ROW(t.*) is the row of t, and ROW(t.*, 9) is that row with one more field on the
            // end. Keeping it whole made a two-column row one field wide.
            if (arr.isRow() && namesAWholeRow(elem) && value instanceof List<?>) {
                list.addAll((List<?>) value);
                continue;
            }
            list.add(value);
        }
        if (arr.isRow()) return new AstExecutor.PgRow(list);
        checkArrayDimensions(list);
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
        rejectMismatchedElementDimensions(arr, list, ctx);
        // Validate element type homogeneity
        if (!arr.isRow() && !list.isEmpty() && !(list.get(0) instanceof List<?>)) {
            Object firstNonNull = null;
            for (Object v : list) { if (v != null) { firstNonNull = v; break; } }
            if (firstNonNull instanceof Number) {
                for (Object v : list) {
                    if (v != null && v instanceof String) {
                        String s = (String) v;
                        // Allow special float values (NaN, Infinity, -Infinity) alongside numeric literals
                        String lower = s.trim().toLowerCase(java.util.Locale.ROOT);
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
        return PgArray.of(list);
    }

    /** Whether the expression is a relation's whole row written out, rather than one value. */
    private static boolean namesAWholeRow(Expression elem) {
        return elem instanceof CompositeStarExpr || elem instanceof WildcardExpr;
    }

    /** PG stores an array's dimension count in a fixed-size header, capped at MAXDIM. */
    private static final int MAX_ARRAY_DIMENSIONS = 6;

    private static void checkArrayDimensions(List<Object> list) {
        int dims = 1;
        Object probe = list.isEmpty() ? null : list.get(0);
        while (probe instanceof List<?>) {
            dims++;
            List<?> inner = (List<?>) probe;
            probe = inner.isEmpty() ? null : inner.get(0);
        }
        if (dims > MAX_ARRAY_DIMENSIONS) {
            throw new MemgresException("number of array dimensions (" + dims
                    + ") exceeds the maximum allowed (" + MAX_ARRAY_DIMENSIONS + ")", "54000");
        }
    }

    /**
     * An ARRAY constructor whose elements are not all the same depth. PostgreSQL resolves the
     * constructor's type from its elements, and an array beside a plain value is a pair of types it
     * cannot reconcile. A NULL is a different matter: it takes the array type happily, but has no
     * dimensions of its own once the array is built.
     */
    private void rejectMismatchedElementDimensions(ArrayExpr arr, List<Object> list, RowContext ctx) {
        boolean anyArray = false;
        for (Object v : list) {
            if (v instanceof List<?>) { anyArray = true; break; }
        }
        if (!anyArray) return;
        String arrayTypeName = null;
        for (Object v : list) {
            if (v instanceof List<?>) {
                arrayTypeName = elementTypeNameOf((List<?>) v) + "[]";
                break;
            }
        }
        for (Object v : list) {
            if (v instanceof List<?>) continue;
            if (v == null) {
                throw new MemgresException("multidimensional arrays must have array expressions"
                        + " with matching dimensions", "2202E");
            }
            String scalar = AstExecutor.pgTypeNameOf(v);
            boolean arrayFirst = list.get(0) instanceof List<?>;
            throw new MemgresException("ARRAY types " + (arrayFirst ? arrayTypeName : scalar)
                    + " and " + (arrayFirst ? scalar : arrayTypeName) + " cannot be matched", "42804");
        }
    }

    /** The element type an array value carries, named from its first leaf. */
    private static String elementTypeNameOf(List<?> array) {
        Object first = firstNonNull(array);
        while (first instanceof List<?>) first = firstNonNull((List<?>) first);
        return AstExecutor.pgTypeNameOf(first);
    }

    Object evalArraySubquery(ArraySubqueryExpr asq, RowContext outerCtx) {
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(asq.subquery());
            // ARRAY(...) collects one column into an array, so a second column has nowhere to go.
            // Taking row[0] and dropping the rest turned ARRAY(SELECT 1, 2) into {1} silently.
            ExprEvaluator.rejectWideSubquery(asq.subquery(), result);
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
