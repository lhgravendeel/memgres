package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SubscriptExpr;
import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.List;

/**
 * Writing through a subscript: {@code SET a[i] = v}, {@code SET a[i][j] = v}, {@code SET a[i:j] = v}
 * and {@code SET b['k'] = v}.
 *
 * <p>The parser used to rewrite every one of these into {@code jsonb_set}. A jsonb path has no
 * lower bound, no rectangular shape and no element type, so writing below an array's lower bound
 * was discarded, writing one element of a two-dimensional array replaced a whole row of it with a
 * scalar, and a slice shorter than the range it was assigned to was padded with nulls instead of
 * being refused. An array is written here as an array.
 */
final class SubscriptAssign {

    private final AstExecutor executor;

    SubscriptAssign(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * The column's new value after the assignment. {@code current} is what the column holds now,
     * which may be null.
     */
    Object assign(Object current, Column column, List<SubscriptExpr.Subscript> subscripts,
            Expression valueExpr, RowContext ctx) {
        if (DataType.isArrayType(column.getType()) || current instanceof List<?>) {
            return assignArray(current, column, subscripts, valueExpr, ctx);
        }
        // An hstore is a map of text to text, so a key written through brackets takes the value as
        // the text it is rather than as a json document.
        if (column.getType() == DataType.HSTORE || current instanceof HstoreValue) {
            return assignHstore(current, subscripts, valueExpr, ctx);
        }
        return assignJson(current, subscripts, valueExpr, ctx);
    }

    /** One key of an hstore. */
    private Object assignHstore(Object current, List<SubscriptExpr.Subscript> subscripts,
            Expression valueExpr, RowContext ctx) {
        HstoreValue hstore = current instanceof HstoreValue ? (HstoreValue) current
                : HstoreValue.parse(current == null ? "" : TypeCoercion.toString(current));
        Object key = subscripts.get(0).lower() == null ? null
                : executor.evalExpr(subscripts.get(0).lower(), ctx);
        if (key == null) return hstore;
        Object value = executor.evalExpr(valueExpr, ctx);
        java.util.Map<String, String> data =
                new java.util.LinkedHashMap<String, String>(hstore.getData());
        data.put(TypeCoercion.toString(key), value == null ? null : TypeCoercion.toString(value));
        return new HstoreValue(data);
    }

    /** An element or a slice of an array, growing the array in either direction as PostgreSQL does. */
    private Object assignArray(Object current, Column column,
            List<SubscriptExpr.Subscript> subscripts, Expression valueExpr, RowContext ctx) {
        DataType elementType = DataType.elementOf(column.getType());
        PgArray array = PgArray.from(current);
        if (array == null) array = PgArray.of(new ArrayList<Object>());
        Object value = executor.evalExpr(valueExpr, ctx);

        boolean slice = false;
        for (SubscriptExpr.Subscript s : subscripts) {
            if (s.slice()) slice = true;
        }
        if (slice) {
            if (subscripts.size() != 1) {
                throw new MemgresException(
                        "multi-dimensional slice assignment is not supported", "0A000");
            }
            return assignSlice(array, subscripts.get(0), value, elementType, ctx);
        }
        int[] indexes = new int[subscripts.size()];
        for (int i = 0; i < subscripts.size(); i++) {
            Object index = executor.evalExpr(subscripts.get(i).lower(), ctx);
            if (index == null) {
                throw new MemgresException("array subscript in assignment must not be null", "22004");
            }
            indexes[i] = executor.toInt(index);
        }
        Object element = elementType == null ? value : TypeCoercion.coerce(value, elementType);
        return writeElement(array, indexes, element);
    }

    /**
     * One element. A subscript outside the array extends it and moves its bounds, which is why the
     * lower bound is part of the value rather than something the text form merely mentions.
     */
    private PgArray writeElement(PgArray array, int[] indexes, Object element) {
        int[] dims = array.dims();
        if (dims.length > 0 && indexes.length != dims.length) {
            throw new MemgresException("wrong number of array subscripts", "2202E");
        }
        if (dims.length > 1) {
            // Only the outermost dimension can grow, so a deeper subscript has to name a position
            // that is already there.
            List<Object> updated = new ArrayList<Object>(array);
            List<Object> level = updated;
            for (int d = 0; d < indexes.length - 1; d++) {
                int offset = indexes[d] - array.lowerBound(d + 1);
                if (offset < 0 || offset >= level.size()) {
                    throw new MemgresException("array subscript out of range", "2202E");
                }
                Object inner = level.get(offset);
                if (!(inner instanceof List<?>)) {
                    throw new MemgresException("array subscript out of range", "2202E");
                }
                List<Object> copy = new ArrayList<Object>((List<?>) inner);
                level.set(offset, copy);
                level = copy;
            }
            int last = indexes[indexes.length - 1] - array.lowerBound(indexes.length);
            if (last < 0 || last >= level.size()) {
                throw new MemgresException("array subscript out of range", "2202E");
            }
            level.set(last, element);
            return array.resized(updated);
        }

        int lower = dims.length == 0 ? indexes[0] : array.lowerBound(1);
        List<Object> elements = new ArrayList<Object>(array);
        int index = indexes[0];
        if (index < lower) {
            // Writing before the start moves the start: the gap between is filled with nulls.
            List<Object> grown = new ArrayList<Object>();
            grown.add(element);
            for (int i = index + 1; i < lower; i++) grown.add(null);
            grown.addAll(elements);
            return PgArray.of(grown, new int[]{index}, array.elementType());
        }
        int offset = index - lower;
        while (elements.size() <= offset) elements.add(null);
        elements.set(offset, element);
        return PgArray.of(elements, new int[]{lower}, array.elementType());
    }

    /** A range of elements, taken from an array that has to be long enough to fill it. */
    private PgArray assignSlice(PgArray array, SubscriptExpr.Subscript subscript, Object value,
            DataType elementType, RowContext ctx) {
        int lower = array.isEmpty() ? 1 : array.lowerBound(1);
        Object loValue = subscript.lower() == null ? null : executor.evalExpr(subscript.lower(), ctx);
        Object hiValue = subscript.upper() == null ? null : executor.evalExpr(subscript.upper(), ctx);
        if ((subscript.lower() != null && loValue == null)
                || (subscript.upper() != null && hiValue == null)) {
            throw new MemgresException("array subscript in assignment must not be null", "22004");
        }
        int from = loValue == null ? lower : executor.toInt(loValue);
        int to = hiValue == null ? lower + array.size() - 1 : executor.toInt(hiValue);

        PgArray source = PgArray.from(value);
        if (source == null) {
            throw new MemgresException("source array too small", "2202E");
        }
        int wanted = to - from + 1;
        if (source.size() < wanted) {
            throw new MemgresException("source array too small", "2202E");
        }
        List<Object> elements = new ArrayList<Object>(array);
        int newLower = Math.min(lower, from);
        if (newLower < lower) {
            List<Object> grown = new ArrayList<Object>();
            for (int i = newLower; i < lower; i++) grown.add(null);
            grown.addAll(elements);
            elements = grown;
        }
        for (int i = 0; i < wanted; i++) {
            int offset = from + i - newLower;
            while (elements.size() <= offset) elements.add(null);
            Object element = source.get(i);
            elements.set(offset, elementType == null ? element
                    : TypeCoercion.coerce(element, elementType));
        }
        return PgArray.of(elements, new int[]{newLower}, array.elementType());
    }

    /**
     * A json key or position. The right-hand side is a jsonb value, not the text of one: assigning
     * {@code '5'} stores the number five, as it would through {@code jsonb_set}.
     */
    private Object assignJson(Object current, List<SubscriptExpr.Subscript> subscripts,
            Expression valueExpr, RowContext ctx) {
        List<Object> path = new ArrayList<Object>();
        for (SubscriptExpr.Subscript s : subscripts) {
            Object key = s.lower() == null ? null : executor.evalExpr(s.lower(), ctx);
            path.add(key == null ? null : TypeCoercion.toString(key));
        }
        StringBuilder pathText = new StringBuilder("{");
        for (int i = 0; i < path.size(); i++) {
            if (i > 0) pathText.append(',');
            TypeCoercion.appendArrayElement(pathText, path.get(i) == null ? "" : path.get(i).toString());
        }
        pathText.append('}');
        // A literal on the right of a json subscript is read by jsonb's input function, so '5' is
        // the number five rather than the one-character string.
        Expression jsonValue = valueExpr instanceof Literal
                && ((Literal) valueExpr).literalType() == Literal.LiteralType.STRING
                ? new com.memgres.engine.parser.ast.CastExpr(valueExpr, "jsonb")
                : new FunctionCallExpr("to_jsonb", Cols.listOf(valueExpr));
        FunctionCallExpr call = new FunctionCallExpr("jsonb_set", Cols.listOf(
                new com.memgres.engine.parser.ast.ComputedValue(current),
                Literal.ofString(pathText.toString()), jsonValue));
        return executor.functionEvaluator.evalFunction(call, ctx);
    }
}
