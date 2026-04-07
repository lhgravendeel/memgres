package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.math.BigDecimal;
import java.util.*;

/**
 * A hash-map based index for a set of columns on a table.
 * Used to accelerate PK/UNIQUE constraint checks and FK lookups.
 *
 * Keys are normalized so that type-equivalent values (e.g. Integer 1 and Long 1)
 * produce the same hash code and compare as equal.
 */
public class TableIndex {

    private final String constraintName;
    private final int[] columnIndices;
    private final boolean unique;

    // For unique indexes: key → single row.  For non-unique: key → list of rows.
    private final HashMap<IndexKey, Object> entries = new HashMap<>();

    public TableIndex(String constraintName, int[] columnIndices, boolean unique) {
        this.constraintName = constraintName;
        this.columnIndices = columnIndices;
        this.unique = unique;
    }

    public String getConstraintName() { return constraintName; }
    public int[] getColumnIndices() { return columnIndices; }
    public boolean isUnique() { return unique; }

    /** Extract and normalize the key values from a row. */
    public IndexKey extractKey(Object[] row) {
        Object[] vals = new Object[columnIndices.length];
        for (int i = 0; i < columnIndices.length; i++) {
            vals[i] = normalize(row[columnIndices[i]]);
        }
        return new IndexKey(vals);
    }

    /** Add a row to the index. */
    public void put(Object[] row) {
        IndexKey key = extractKey(row);
        if (unique) {
            // Skip NULL keys in unique indexes. NULLs are distinct, so they never
            // participate in conflict checks, and storing them would cause collisions
            // when multiple rows have NULL in the indexed column.
            for (Object v : key.values) {
                if (v == null) return;
            }
            entries.put(key, row);
        } else {
            @SuppressWarnings("unchecked")
            List<Object[]> list = (List<Object[]>) entries.get(key);
            if (list == null) {
                list = new ArrayList<>(2);
                entries.put(key, list);
            }
            list.add(row);
        }
    }

    /** Remove a row from the index using the current column values in the row. */
    public void remove(Object[] row) {
        removeByKey(extractKey(row), row);
    }

    /** Remove a row from the index using explicitly provided old key values. */
    public void removeByOldValues(Object[] oldValues, Object[] row) {
        Object[] vals = new Object[columnIndices.length];
        for (int i = 0; i < columnIndices.length; i++) {
            vals[i] = normalize(oldValues[columnIndices[i]]);
        }
        removeByKey(new IndexKey(vals), row);
    }

    private void removeByKey(IndexKey key, Object[] row) {
        if (unique) {
            Object existing = entries.get(key);
            if (existing == row) {
                entries.remove(key);
            }
        } else {
            @SuppressWarnings("unchecked")
            List<Object[]> list = (List<Object[]>) entries.get(key);
            if (list != null) {
                list.removeIf(r -> r == row);
                if (list.isEmpty()) {
                    entries.remove(key);
                }
            }
        }
    }

    /**
     * For unique indexes: check if a key already exists (excluding a specific row).
     * Returns the conflicting row, or null if no conflict.
     */
    public Object[] findConflict(Object[] row, Object[] excludeRow) {
        IndexKey key = extractKey(row);
        // If any key column is null, no conflict (NULLs are distinct by default)
        for (Object v : key.values) {
            if (v == null) return null;
        }
        Object existing = entries.get(key);
        if (existing == null) return null;
        if (unique) {
            Object[] found = (Object[]) existing;
            return (found != excludeRow) ? found : null;
        } else {
            @SuppressWarnings("unchecked")
            List<Object[]> list = (List<Object[]>) existing;
            for (Object[] found : list) {
                if (found != excludeRow) return found;
            }
            return null;
        }
    }

    /**
     * Look up a row by key values (for FK validation).
     * Returns true if at least one matching row exists.
     */
    public boolean containsKey(Object[] keyValues) {
        IndexKey key = makeKey(keyValues);
        return entries.containsKey(key);
    }

    /**
     * Find all rows matching the given key values (for FK CASCADE).
     * Returns empty list if none found.
     */
    public List<Object[]> findAll(Object[] keyValues) {
        IndexKey key = makeKey(keyValues);
        Object existing = entries.get(key);
        if (existing == null) return Cols.listOf();
        if (unique) {
            Object[] row = (Object[]) existing;
            return Collections.singletonList(row);
        } else {
            @SuppressWarnings("unchecked")
            List<Object[]> list = (List<Object[]>) existing;
            return new ArrayList<>(list);
        }
    }

    /** Clear all entries. */
    public void clear() {
        entries.clear();
    }

    /** Number of distinct keys in the index. */
    public int size() {
        return entries.size();
    }

    private IndexKey makeKey(Object[] keyValues) {
        Object[] vals = new Object[keyValues.length];
        for (int i = 0; i < keyValues.length; i++) {
            vals[i] = normalize(keyValues[i]);
        }
        return new IndexKey(vals);
    }

    /**
     * Normalize a value for use as a HashMap key.
     * This ensures type-equivalent values produce the same hashCode and equals behavior.
     */
    static Object normalize(Object val) {
        if (val == null) return null;
        if (val instanceof Number) {
            Number n = (Number) val;
            // Normalize all numeric types to BigDecimal for consistent hashing
            if (val instanceof BigDecimal) return ((BigDecimal) val).stripTrailingZeros();
            if (val instanceof Double) {
                Double d = (Double) val;
                if (d.isNaN() || d.isInfinite()) return d; // NaN/Infinity can't be BigDecimal
                return BigDecimal.valueOf(d).stripTrailingZeros();
            }
            if (val instanceof Float) {
                Float f = (Float) val;
                if (f.isNaN() || f.isInfinite()) return f.doubleValue(); // normalize Float specials to Double
                return BigDecimal.valueOf(f.doubleValue()).stripTrailingZeros();
            }
            return BigDecimal.valueOf(n.longValue());
        }
        if (val instanceof byte[]) {
            byte[] ba = (byte[]) val;
            // byte arrays don't have value-based hashCode, wrap them
            return new ByteArrayKey(ba);
        }
        return val;
    }

    /**
     * Index key: a tuple of normalized values with proper equals/hashCode.
     */
        public static final class IndexKey {
        public final Object[] values;

        public IndexKey(Object[] values) {
            this.values = values;
        }

        @Override
        public int hashCode() {
            int h = 1;
            for (Object v : values) {
                if (v instanceof ByteArrayKey) {
                    ByteArrayKey bk = (ByteArrayKey) v;
                    h = 31 * h + Arrays.hashCode(bk.data);
                } else {
                    h = 31 * h + (v == null ? 0 : v.hashCode());
                }
            }
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof IndexKey)) return false;
            IndexKey other = (IndexKey) obj;
            if (values.length != other.values.length) return false;
            for (int i = 0; i < values.length; i++) {
                if (!indexValEquals(values[i], other.values[i])) return false;
            }
            return true;
        }

        private static boolean indexValEquals(Object a, Object b) {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a instanceof BigDecimal && b instanceof BigDecimal) {
                BigDecimal bb = (BigDecimal) b;
                BigDecimal ba = (BigDecimal) a;
                return ba.compareTo(bb) == 0;
            }
            if (a instanceof ByteArrayKey && b instanceof ByteArrayKey) {
                ByteArrayKey bb = (ByteArrayKey) b;
                ByteArrayKey ba = (ByteArrayKey) a;
                return Arrays.equals(ba.data, bb.data);
            }
            return a.equals(b);
        }

        public Object[] values() { return values; }

        @Override
        public String toString() {
            return "IndexKey[values=" + java.util.Arrays.toString(values) + "]";
        }
    }

    /** Wrapper for byte[] with value-based equals/hashCode. */
        public static final class ByteArrayKey {
        public final byte[] data;

        public ByteArrayKey(byte[] data) {
            this.data = data;
        }

        @Override
        public int hashCode() { return Arrays.hashCode(data); }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof ByteArrayKey && Arrays.equals(data, ((ByteArrayKey) obj).data);
        }

        public byte[] data() { return data; }

        @Override
        public String toString() {
            return "ByteArrayKey[data=" + java.util.Arrays.toString(data) + "]";
        }
    }
}
