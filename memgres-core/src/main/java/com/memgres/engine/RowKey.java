package com.memgres.engine;

import java.math.BigDecimal;
import java.util.Arrays;

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
        this.values = row;
        int h = 1;
        for (Object v : row) {
            h = 31 * h + normalizedHashCode(v);
        }
        this.hash = h;
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
        return a.equals(b);
    }

    private static int normalizedHashCode(Object v) {
        if (v == null) return 0;
        if (v instanceof BigDecimal) {
            // stripTrailingZeros so 1.0 and 1.00 have the same hash
            return ((BigDecimal) v).stripTrailingZeros().hashCode();
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

    /** Compute a value-based key string for a single value (for GROUP BY). */
    static String valueKey(Object val) {
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
        // Use NUL-prefixed type tag to prevent cross-type/cross-column collisions
        return "\0V" + val.getClass().getName() + "\0" + val;
    }
}
