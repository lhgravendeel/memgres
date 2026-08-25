package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Value-based key for row equality comparison in DISTINCT, UNION/INTERSECT/EXCEPT,
 * and GROUP BY. Uses proper value equality instead of string representation.
 *
 * This avoids two classes of bugs:
 * 1. False collisions: ("a, b", "c") and ("a", "b, c") produce the same
 *    deepToString but are different rows.
 * 2. False splits: 1.0 and 1.00 produce different toString but are equal numerics.
 */
final class RowKey {
    private final Object[] values;
    private final int hash;

    RowKey(Object[] row) {
        this(row, null);
    }

    /**
     * A key over a row whose columns' types are known.
     *
     * <p>Almost every type says everything about a value in the value itself, but jsonb does not:
     * it is held as the text it prints as, where the value is the document that text spells, and
     * the two are not in step — {@code 1} and {@code 1.0} are one number written twice, and an
     * object's members are one set however they were ordered. So a jsonb column is keyed by its
     * document rather than by its text, and every other column by the value alone.
     *
     * @param types one entry per column, or null when the caller has no types to offer
     */
    RowKey(Object[] row, DataType[] types) {
        Object[] keys = row;
        if (types != null) {
            keys = new Object[row.length];
            for (int i = 0; i < row.length; i++) {
                keys[i] = i < types.length ? keyValue(row[i], types[i]) : row[i];
            }
        }
        this.values = keys;
        int h = 1;
        for (Object v : keys) {
            h = 31 * h + normalizedHashCode(v);
        }
        this.hash = h;
    }

    /**
     * The value a column of this type is compared by, which is the value itself unless its type
     * holds more than the value shows.
     */
    private static Object keyValue(Object val, DataType type) {
        if (type == DataType.JSONB && val instanceof String) {
            return JsonOperations.jsonbKey((String) val);
        }
        return val;
    }

    /**
     * The types the columns of a result have, or null when none of them is keyed by anything but
     * its own value — which is almost always, so the rows are then keyed without a second array.
     */
    static DataType[] columnTypes(List<Column> columns) {
        if (columns == null) return null;
        boolean any = false;
        DataType[] types = new DataType[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            types[i] = columns.get(i).getType();
            if (types[i] == DataType.JSONB) any = true;
        }
        return any ? types : null;
    }

    /**
     * Refuses a result whose rows cannot be gathered into groups, because one of the columns they
     * would be grouped by has no equality operator.
     */
    static void requireEquality(List<Column> columns) {
        if (columns == null) return;
        for (Column column : columns) {
            MemgresException e = OperatorResolution.noEqualityFor(column.getType());
            if (e != null) throw e;
        }
    }

    /**
     * The types a list of key expressions has, for the clauses that gather rows by expressions
     * rather than by whole rows — GROUP BY, DISTINCT ON and a window's PARTITION BY. Each of them
     * needs an equality over its keys, and each of them keys a jsonb by the document rather than
     * by its text, so both are settled here from the one type.
     *
     * @return one entry per expression, or null when none of them is keyed by anything but its
     *         own value
     */
    static DataType[] keyTypes(AstExecutor executor, List<Expression> exprs,
                               List<RowContext.TableBinding> bindings) {
        if (exprs == null || exprs.isEmpty()) return null;
        boolean any = false;
        DataType[] types = new DataType[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            types[i] = executor.exprEvaluator.inferTypeFromContext(exprs.get(i), bindings);
            MemgresException e = OperatorResolution.noEqualityFor(types[i]);
            if (e != null) throw e;
            if (types[i] == DataType.JSONB) any = true;
        }
        return any ? types : null;
    }

    /** The key of the {@code i}th of a list of key expressions, typed by {@link #keyTypes}. */
    static String keyOf(Object val, DataType[] types, int i) {
        return types == null || i >= types.length ? valueKey(val) : valueKey(val, types[i]);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RowKey)) return false;
        RowKey other = (RowKey) obj;
        if (values.length != other.values.length) return false;
        for (int i = 0; i < values.length; i++) {
            if (!valuesEqual(values[i], other.values[i])) return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean valuesEqual(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        // Normalize numeric comparison: 1.0 == 1.00
        if (a instanceof BigDecimal && b instanceof BigDecimal) {
            return ((BigDecimal) a).compareTo((BigDecimal) b) == 0;
        }
        // Cross-type numeric: BigDecimal vs integer types
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            try {
                BigDecimal da = toBigDecimal(a);
                BigDecimal db = toBigDecimal(b);
                if (da != null && db != null) return da.compareTo(db) == 0;
            } catch (Exception ignored) {}
        }
        // bytea is stored as byte[], whose identity equals() would make every value its
        // own group; PG groups equal byte sequences together.
        if (a instanceof byte[] && b instanceof byte[]) {
            return java.util.Arrays.equals((byte[]) a, (byte[]) b);
        }
        // Array comparison
        if (a instanceof Object[] && b instanceof Object[]) {
            Object[] aa = (Object[]) a;
            Object[] ba = (Object[]) b;
            if (aa.length != ba.length) return false;
            for (int i = 0; i < aa.length; i++) {
                if (!valuesEqual(aa[i], ba[i])) return false;
            }
            return true;
        }
        if (a instanceof java.util.List && b instanceof java.util.List) {
            java.util.List<?> la = (java.util.List<?>) a;
            java.util.List<?> lb = (java.util.List<?>) b;
            if (la.size() != lb.size()) return false;
            for (int i = 0; i < la.size(); i++) {
                if (!valuesEqual(la.get(i), lb.get(i))) return false;
            }
            return true;
        }
        // The types whose Java equality is not the type's =. A timestamptz is the instant it
        // names, whatever offset it was written with; an interval is the span it measures,
        // whatever fields it was spelled in; and IEEE's two zeros are one number.
        if (a instanceof java.time.OffsetDateTime && b instanceof java.time.OffsetDateTime) {
            return ((java.time.OffsetDateTime) a).toInstant()
                    .equals(((java.time.OffsetDateTime) b).toInstant());
        }
        if (a instanceof PgInterval && b instanceof PgInterval) {
            return TypeCoercion.areEqual(a, b);
        }
        if ((a instanceof Double || a instanceof Float) && (b instanceof Double || b instanceof Float)) {
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            return Double.isNaN(da) && Double.isNaN(db) ? true : da == db;
        }
        return a.equals(b);
    }

    private static int normalizedHashCode(Object v) {
        if (v == null) return 0;
        if (v instanceof byte[]) return java.util.Arrays.hashCode((byte[]) v);
        if (v instanceof BigDecimal) {
            // stripTrailingZeros so 1.0 and 1.00 have the same hash
            return ((BigDecimal) v).stripTrailingZeros().hashCode();
        }
        if (v instanceof java.time.OffsetDateTime) {
            return ((java.time.OffsetDateTime) v).toInstant().hashCode();
        }
        if (v instanceof PgInterval) {
            PgInterval interval = (PgInterval) v;
            return interval.isInfinite() ? interval.toString().hashCode()
                    : Long.valueOf(interval.normalizedMicroseconds()).hashCode();
        }
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            return Double.valueOf(d == 0.0 ? 0.0 : d).hashCode();
        }
        if (v instanceof Object[]) {
            int h = 1;
            for (Object e : (Object[]) v) h = 31 * h + normalizedHashCode(e);
            return h;
        }
        if (v instanceof java.util.List) {
            int h = 1;
            for (Object e : (java.util.List<?>) v) h = 31 * h + normalizedHashCode(e);
            return h;
        }
        return v.hashCode();
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof Integer) return BigDecimal.valueOf((Integer) v);
        if (v instanceof Long) return BigDecimal.valueOf((Long) v);
        if (v instanceof Double) return BigDecimal.valueOf((Double) v);
        if (v instanceof Float) return BigDecimal.valueOf((Float) v);
        if (v instanceof Short) return BigDecimal.valueOf((Short) v);
        return null;
    }

    /**
     * The key a single value of a known type has. jsonb is the one type held as something other
     * than its value — see {@link #RowKey(Object[], DataType[])} — so it is the one type whose key
     * the value alone cannot give.
     */
    static String valueKey(Object val, DataType type) {
        if (type == DataType.JSONB && val instanceof String) {
            return "\0JSB" + JsonOperations.jsonbKey((String) val);
        }
        return valueKey(val);
    }

    /** Compute a value-based key string for a single value (for GROUP BY). */
    static String valueKey(Object val) {
        // bytea: identity toString() would give every value its own grouping key
        if (val instanceof byte[]) {
            byte[] b = (byte[]) val;
            StringBuilder sb = new StringBuilder(b.length * 2 + 4).append("\0BYT");
            for (byte x : b) {
                sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
            }
            return sb.toString();
        }
        if (val == null) return "\0NULL";
        if (val instanceof BigDecimal) {
            return ((BigDecimal) val).stripTrailingZeros().toPlainString();
        }
        if (val instanceof Object[]) {
            StringBuilder sb = new StringBuilder("\0ARR[");
            for (Object e : (Object[]) val) {
                sb.append(valueKey(e)).append('\0');
            }
            return sb.append(']').toString();
        }
        if (val instanceof java.util.List) {
            StringBuilder sb = new StringBuilder("\0LST[");
            for (Object e : (java.util.List<?>) val) {
                sb.append(valueKey(e)).append('\0');
            }
            return sb.append(']').toString();
        }
        // An interval is the span it measures, and two spellings of one span are one value:
        // '1 mon', '30 days' and '720 hours' are the same interval, so they group together. The
        // written form made three keys where PostgreSQL counts one.
        if (val instanceof PgInterval) {
            PgInterval interval = (PgInterval) val;
            return "\0IVL" + (interval.isInfinite() ? interval.toString()
                    : String.valueOf(interval.normalizedMicroseconds()));
        }
        // A number is the number it is, whatever width it is held in, so 1 and 1.0 are one value
        // — which is what lets the two branches of a set operation meet.
        if (val instanceof Integer || val instanceof Long || val instanceof Short
                || val instanceof java.math.BigInteger) {
            return new BigDecimal(val.toString()).stripTrailingZeros().toPlainString();
        }
        // A timestamptz is the instant it names, so two spellings of one instant are one value.
        if (val instanceof java.time.OffsetDateTime) {
            return "\0TSZ" + ((java.time.OffsetDateTime) val).toInstant();
        }
        // IEEE's negative zero is the same number as its positive one, and = says so.
        if (val instanceof Double || val instanceof Float) {
            double d = ((Number) val).doubleValue();
            return "\0FLT" + (d == 0.0 ? 0.0 : d);
        }
        // A composite is its fields, written the way record_out writes them, so a comma inside a
        // field cannot be mistaken for the boundary between two.
        if (val instanceof AstExecutor.PgRow) {
            return "\0ROW" + ((AstExecutor.PgRow) val).toPgText();
        }
        // Use NUL-prefixed type tag to prevent cross-type/cross-column collisions
        return "\0V" + val.getClass().getName() + "\0" + val;
    }
}
