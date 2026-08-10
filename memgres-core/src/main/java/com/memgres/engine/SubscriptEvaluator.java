package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SubscriptExpr;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates {@code a[i]}, {@code a[i][j]} and {@code a[i:j]}.
 *
 * <p>PostgreSQL gives four kinds of value a subscript, and gives them different rules: an array is
 * indexed from its own lower bound and rounds a fractional index; a jsonb container is indexed from
 * zero and counts a negative index back from the end; an hstore is indexed by key; a geometric
 * value is indexed by part. Everything else has no subscript at all, and saying so is an error
 * about the type rather than about an operator.
 */
final class SubscriptEvaluator {

    private final AstExecutor executor;

    SubscriptEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(SubscriptExpr expr, RowContext ctx) {
        Object base = executor.evalExpr(expr.base(), ctx);
        if (base == null) return null;
        List<SubscriptExpr.Subscript> subscripts = expr.subscripts();

        if (base instanceof HstoreValue) {
            Object key = indexValue(subscripts.get(0).lower(), ctx);
            return key == null ? null : ((HstoreValue) base).get(TypeCoercion.toString(key));
        }
        if (base instanceof PgVector) {
            Integer index = arrayIndex(subscripts.get(0).lower(), ctx);
            if (index == null) return null;
            PgVector vector = (PgVector) base;
            return index >= 0 && index < vector.size() ? vector.get(index) : null;
        }
        PgArray array = base instanceof List<?> ? PgArray.from(base) : null;
        if (array == null && base instanceof String) {
            array = arrayFromText(expr.base(), (String) base, ctx);
        }
        if (array != null) return subscriptArray(array, subscripts, ctx);

        String typeName = declaredTypeName(expr.base(), base, ctx);
        if (isJsonb(typeName, base)) return subscriptJson(base.toString(), subscripts, ctx);
        if (GeometricOperations.isSubscriptableType(typeName)) {
            Integer index = arrayIndex(subscripts.get(0).lower(), ctx);
            return index == null ? null
                    : GeometricOperations.partAt(typeName, base.toString(), index);
        }
        // PG's name type is subscriptable by character, counting from zero; the catalogs are read
        // that way (typname[0] = '_'), so this is not a fallback but a type with its own rule.
        if ("name".equals(typeName)) {
            Integer index = arrayIndex(subscripts.get(0).lower(), ctx);
            String text = base.toString();
            if (index == null || index < 0 || index >= text.length()) return null;
            return String.valueOf(text.charAt(index));
        }
        throw new MemgresException("cannot subscript type " + (typeName == null ? "text" : typeName)
                + " because it does not support subscripting", "42804");
    }

    /**
     * A subscript into an array. A reference that names fewer or more dimensions than the array has
     * selects nothing; a range selects a whole array, over as many dimensions as it names.
     */
    private Object subscriptArray(PgArray array, List<SubscriptExpr.Subscript> subscripts,
            RowContext ctx) {
        if (anySlice(subscripts)) return sliceArray(array, subscripts, ctx);
        int[] dims = array.dims();
        if (subscripts.size() != dims.length) return null;
        Object current = array;
        for (int i = 0; i < subscripts.size(); i++) {
            Integer index = arrayIndex(subscripts.get(i).lower(), ctx);
            if (index == null) return null;
            List<?> level = (List<?>) current;
            int offset = index - lowerBoundOf(array, i + 1);
            if (offset < 0 || offset >= level.size()) return null;
            current = level.get(offset);
            if (current == null) return null;
        }
        return current;
    }

    /** A range over one or more dimensions, each clipped to what the array holds. */
    private Object sliceArray(PgArray array, List<SubscriptExpr.Subscript> subscripts,
            RowContext ctx) {
        List<Object> sliced = sliceLevel(array, subscripts, 0, array, ctx);
        if (sliced == null) return null;
        return PgArray.ofType(sliced, array.elementType());
    }

    private List<Object> sliceLevel(List<?> level, List<SubscriptExpr.Subscript> subscripts,
            int depth, PgArray array, RowContext ctx) {
        if (depth >= subscripts.size()) return new ArrayList<Object>(level);
        SubscriptExpr.Subscript subscript = subscripts.get(depth);
        int lowerBound = lowerBoundOf(array, depth + 1);
        Integer lower = subscript.lower() == null ? null : arrayIndex(subscript.lower(), ctx);
        Integer upper = subscript.upper() == null ? null : arrayIndex(subscript.upper(), ctx);
        // A bound that is present but NULL leaves no range at all, so the whole reference is NULL.
        if (subscript.lower() != null && lower == null) return null;
        if (subscript.upper() != null && upper == null) return null;
        // A single index inside a slice reference selects one position, as [i:i] would.
        if (!subscript.slice() && lower != null) upper = lower;
        int from = lower == null ? 0 : lower - lowerBound;
        int to = upper == null ? level.size() - 1 : upper - lowerBound;
        from = Math.max(from, 0);
        to = Math.min(to, level.size() - 1);
        List<Object> out = new ArrayList<Object>();
        for (int i = from; i <= to; i++) {
            Object element = level.get(i);
            if (depth + 1 < subscripts.size() && element instanceof List<?>) {
                List<Object> inner = sliceLevel((List<?>) element, subscripts, depth + 1, array, ctx);
                if (inner == null) return null;
                out.add(inner);
            } else {
                out.add(element);
            }
        }
        return out;
    }

    /** A subscript into a json container: keys for an object, zero-based positions for an array. */
    private Object subscriptJson(String base, List<SubscriptExpr.Subscript> subscripts,
            RowContext ctx) {
        String current = base;
        for (SubscriptExpr.Subscript subscript : subscripts) {
            if (current == null) return null;
            Object key = indexValue(subscript.lower(), ctx);
            if (key == null) return null;
            if (key instanceof Number && JsonOperations.isArray(current.trim())) {
                List<String> elements = JsonOperations.parseArrayElements(current.trim());
                int index = ((Number) key).intValue();
                // A negative subscript counts back from the end of the array.
                if (index < 0) index += elements.size();
                current = index >= 0 && index < elements.size() ? elements.get(index) : null;
            } else {
                current = executor.functionEvaluator.extractJsonKey(current,
                        TypeCoercion.toString(key));
            }
        }
        return current;
    }

    private static boolean anySlice(List<SubscriptExpr.Subscript> subscripts) {
        for (SubscriptExpr.Subscript s : subscripts) {
            if (s.slice()) return true;
        }
        return false;
    }

    private static int lowerBoundOf(PgArray array, int dimension) {
        return array.lowerBound(dimension);
    }

    private Object indexValue(Expression expr, RowContext ctx) {
        return expr == null ? null : executor.evalExpr(expr, ctx);
    }

    /**
     * A subscript read as an array index. PostgreSQL rounds a fractional one to the nearest whole
     * number, refuses one that no integer can hold, and refuses a subscript whose type is not a
     * number at all — none of which follows from narrowing the value with intValue().
     */
    private Integer arrayIndex(Expression expr, RowContext ctx) {
        Object value = indexValue(expr, ctx);
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Short) return ((Short) value).intValue();
        if (value instanceof Number) {
            BigDecimal decimal = TypeCoercion.toBigDecimal(value);
            BigDecimal rounded = decimal.setScale(0, RoundingMode.HALF_UP);
            if (rounded.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0
                    || rounded.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
                throw new MemgresException("integer out of range", "22003");
            }
            return rounded.intValue();
        }
        if (value instanceof String) {
            // A bare literal is read as an integer; a value that is already text is not one.
            if (expr instanceof Literal
                    && ((Literal) expr).literalType() == Literal.LiteralType.STRING) {
                return TypeCoercion.toInteger(value);
            }
            throw new MemgresException("array subscript must have type integer", "42804");
        }
        throw new MemgresException("array subscript must have type integer", "42804");
    }

    /** An array held as the text of its literal, when the expression says it is an array. */
    private PgArray arrayFromText(Expression base, String text, RowContext ctx) {
        if (!PgArray.looksLikeArrayText(text)) return null;
        String typeName = declaredTypeName(base, text, ctx);
        if (typeName != null && (typeName.startsWith("json") || typeName.equals("text")
                || typeName.equals("hstore"))) {
            return null;
        }
        return PgArray.from(text);
    }

    private boolean isJsonb(String typeName, Object base) {
        if ("json".equals(typeName)) {
            throw new MemgresException(
                    "cannot subscript type json because it does not support subscripting", "42804");
        }
        if ("jsonb".equals(typeName)) return true;
        if (!(base instanceof String)) return false;
        String text = ((String) base).trim();
        return typeName == null && (JsonOperations.isObject(text) || JsonOperations.isArray(text));
    }

    /** What the expression says its type is, as PostgreSQL would name it, or null when unsettled. */
    private String declaredTypeName(Expression expr, Object value, RowContext ctx) {
        String declared = executor.binaryOpEvaluator.declaredTypeForResolution(expr, ctx);
        if (declared != null) return declared.toLowerCase();
        DataType inferred = executor.inferExprType(expr);
        return inferred == null ? null : inferred.getPgName();
    }
}
