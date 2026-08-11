package com.memgres.engine;

import com.memgres.engine.parser.ast.SelectStmt;

import java.util.List;

/**
 * Partition routing and FROM-item alias extraction helpers.
 * Extracted from DmlExecutor to separate partition concerns.
 */
class DmlPartitionHelper {

    private final AstExecutor executor;

    DmlPartitionHelper(AstExecutor executor) {
        this.executor = executor;
    }

    /** Extract the effective alias from a FROM item (alias if present, else table name). */
    String extractFromItemAlias(SelectStmt.FromItem fi) {
        if (fi instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
            return tr.alias() != null ? tr.alias() : tr.table();
        } else if (fi instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sq = (SelectStmt.SubqueryFrom) fi;
            return sq.alias();
        } else if (fi instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom ff = (SelectStmt.FunctionFrom) fi;
            return ff.alias();
        }
        return null;
    }

    /** Marker for a key the row cannot supply, which stops routing rather than guessing at one. */
    private static final Object NO_KEY = new Object();

    /**
     * The partition key of a row, read with the column positions of the table that holds it.
     * A partition may order its columns differently from the partitioned table it belongs to, so
     * the key column is looked up in the layout the row is actually in.
     */
    private Object partitionKey(Table parent, Table layout, Object[] row) {
        String partCol = parent.getPartitionColumn();
        if (partCol == null) return NO_KEY;
        if (partCol.contains("(")) {
            // Expression-based partition key (e.g., "(lower(s))")
            // Build a row context and evaluate the expression
            return evaluatePartitionExpression(layout, partCol, row);
        }
        if (partCol.contains(",")) {
            // Multi-column partition key: build a tuple in key order
            String[] keyCols = partCol.split(",");
            List<Object> tuple = new java.util.ArrayList<>(keyCols.length);
            for (String keyCol : keyCols) {
                int colIdx = layout.getColumnIndex(keyCol.trim());
                if (colIdx < 0 || colIdx >= row.length) return NO_KEY;
                tuple.add(row[colIdx]);
            }
            return tuple;
        }
        int colIdx = layout.getColumnIndex(partCol);
        if (colIdx < 0 || colIdx >= row.length) return NO_KEY;
        return row[colIdx];
    }

    /**
     * Whether a partition's declared bound claims this key. Routing and the check a write aimed
     * straight at a partition has to pass both read the bound through here, so the two cannot
     * drift: a row belongs in a partition exactly when routing would have sent it there.
     *
     * @param rowHash the key's PostgreSQL hash, worked out once per row by the caller and unused
     *     by the RANGE and LIST strategies
     */
    private boolean boundHolds(Table parent, Table partition, Object value, long rowHash) {
        String strategy = parent.getPartitionStrategy();
        if (strategy == null) return false;
        switch (strategy.toUpperCase()) {
            case "RANGE":
                // NULL keys never match a range partition (they go to DEFAULT or error)
                if (containsNull(value) || partition.getPartitionLower() == null
                        || partition.getPartitionUpper() == null) {
                    return false;
                }
                return compareToBound(value, partition.getPartitionLower()) >= 0
                        && compareToBound(value, partition.getPartitionUpper()) < 0;
            case "LIST":
                if (partition.getPartitionValues() == null) return false;
                for (Object pv : partition.getPartitionValues()) {
                    if (value == null || pv == null) {
                        // SQL NULL routes to a LIST partition declaring NULL in its IN-list
                        if (value == null && pv == null) return true;
                    } else if (executor.compareValues(value, pv) == 0) {
                        return true;
                    }
                }
                return false;
            case "HASH":
                if (partition.getPartitionModulus() == null
                        || partition.getPartitionRemainder() == null
                        || partition.getPartitionModulus() <= 0) {
                    return false;
                }
                return Long.remainderUnsigned(rowHash, partition.getPartitionModulus())
                        == partition.getPartitionRemainder().longValue();
            default:
                return false;
        }
    }

    /**
     * Whether this partition may hold a row with this key: its own bound claims it or, for the
     * DEFAULT partition, no sibling's bound does. PostgreSQL states the default partition's
     * constraint as exactly that negation, so a row some other partition would have taken cannot
     * be written into the default one.
     */
    private boolean claimedBy(Table parent, Table partition, Object value) {
        long rowHash = "HASH".equalsIgnoreCase(parent.getPartitionStrategy())
                ? pgHashRow(value) : 0L;
        if (partition.isDefaultPartition()) {
            for (Table sibling : parent.getPartitions()) {
                if (sibling == partition || sibling.isDefaultPartition()) continue;
                if (boundHolds(parent, sibling, value, rowHash)) return false;
            }
            return true;
        }
        return boundHolds(parent, partition, value, rowHash);
    }

    /**
     * Enforce, on a write whose target is already a partition, the bound that partition was
     * declared with. A partition is stored as an ordinary table and nothing else on the write path
     * consults its bound, so a statement naming it directly would otherwise store a row the
     * partition can never have held. PostgreSQL tests the whole chain of ancestors — a row written
     * into a sub-partition satisfies every bound above it as well — and names the relation the
     * statement wrote to.
     */
    void checkPartitionConstraint(Table leaf, Object[] row) {
        if (leaf == null || row == null || leaf.getPartitionParent() == null) return;
        Table child = leaf;
        Object[] childRow = row;
        for (Table parent = child.getPartitionParent(); parent != null;
                parent = child.getPartitionParent()) {
            Object value = partitionKey(parent, child, childRow);
            if (value != NO_KEY && !claimedBy(parent, child, value)) {
                MemgresException ex = new MemgresException("new row for relation \""
                        + leaf.getName() + "\" violates partition constraint", "23514");
                ex.setTable(leaf.getName());
                ex.setDetail("Failing row contains ("
                        + executor.constraintValidator.formatRow(leaf, row) + ").");
                throw ex;
            }
            childRow = child.rowToParent(childRow);
            child = parent;
        }
    }

    /** Route an INSERT row to the correct partition, or return the table itself if not partitioned. */
    Table routeToPartition(Table table, Object[] row) {
        if (table.getPartitionStrategy() == null) return table;

        String partCol = table.getPartitionColumn();
        if (partCol == null) return table;
        Object value = partitionKey(table, table, row);
        if (value == NO_KEY) return table;

        // The hash of a key does not depend on which partition is being tested, so it is worked
        // out once for the row rather than once round the loop.
        long rowHash = "HASH".equalsIgnoreCase(table.getPartitionStrategy())
                ? pgHashRow(value) : 0L;
        Table matched = null;
        for (Table partition : table.getPartitions()) {
            if (boundHolds(table, partition, value, rowHash)) {
                matched = partition;
                break;
            }
        }
        // Fall back to DEFAULT partition
        if (matched == null) {
            for (Table partition : table.getPartitions()) {
                if (partition.isDefaultPartition()) { matched = partition; break; }
            }
        }
        if (matched == null) {
            MemgresException ex = new MemgresException("no partition of relation \""
                    + table.getName() + "\" found for row", "23514");
            ex.setDetail(partitionKeyDetail(partCol, value));
            throw ex;
        }
        // Recurse for multi-level partitioning (sub-partitions)
        if (matched.getPartitionStrategy() != null && !matched.getPartitions().isEmpty()) {
            return routeToPartition(matched, row);
        }
        return matched;
    }

    /**
     * The key that found no home. The row may be wide and only its key decides where it goes, so
     * PostgreSQL prints the key rather than the row: the reader sees at once which value sits
     * outside every partition. An expression key is printed as the expression it was declared as.
     */
    private static String partitionKeyDetail(String partCol, Object value) {
        StringBuilder names = new StringBuilder();
        if (partCol.startsWith("(")) {
            names.append(partCol);
        } else {
            names.append('(');
            String[] parts = partCol.split(",");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) names.append(", ");
                names.append(parts[i].trim());
            }
            names.append(')');
        }
        StringBuilder vals = new StringBuilder("(");
        if (value instanceof List) {
            List<?> tuple = (List<?>) value;
            for (int i = 0; i < tuple.size(); i++) {
                if (i > 0) vals.append(", ");
                vals.append(tuple.get(i) == null ? "null" : tuple.get(i));
            }
        } else {
            vals.append(value == null ? "null" : value);
        }
        vals.append(')');
        return "Partition key of the failing row contains " + names + " = " + vals + ".";
    }

    /** Whether a routing key value is (or, for tuples, contains) SQL NULL. */
    private static boolean containsNull(Object value) {
        if (value == null) return true;
        if (value instanceof List) {
            for (Object v : (List<?>) value) {
                if (v == null) return true;
            }
        }
        return false;
    }

    /**
     * Compare a row's partition key value against a partition bound.
     * MINVALUE/MAXVALUE sentinels compare below/above every value regardless of key type;
     * multi-column keys compare lexicographically element by element.
     */
    private int compareToBound(Object value, Object bound) {
        if (bound == PartitionBound.MINVALUE) return 1;   // every value is above MINVALUE
        if (bound == PartitionBound.MAXVALUE) return -1;  // every value is below MAXVALUE
        if (value instanceof List && bound instanceof List) {
            List<?> lv = (List<?>) value;
            List<?> lb = (List<?>) bound;
            int minLen = Math.min(lv.size(), lb.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = compareToBound(lv.get(i), lb.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(lv.size(), lb.size());
        }
        return executor.compareValues(value, bound);
    }

    /**
     * Evaluate an expression-based partition key against a row.
     * E.g., for PARTITION BY RANGE ((lower(s))), evaluate lower(s) for the given row.
     */
    private Object evaluatePartitionExpression(Table table, String exprStr, Object[] row) {
        // Strip outer parens: "(lower(s))" -> "lower(s)"
        String inner = exprStr.trim();
        if (inner.startsWith("(") && inner.endsWith(")")) {
            inner = inner.substring(1, inner.length() - 1).trim();
        }
        try {
            // Parse via "SELECT <expr>" and extract the expression from the projection
            com.memgres.engine.parser.ast.Statement stmt =
                    com.memgres.engine.parser.Parser.parse("SELECT " + inner);
            if (stmt instanceof com.memgres.engine.parser.ast.SelectStmt) {
                com.memgres.engine.parser.ast.SelectStmt sel =
                        (com.memgres.engine.parser.ast.SelectStmt) stmt;
                if (sel.targets != null && !sel.targets.isEmpty()) {
                    com.memgres.engine.parser.ast.Expression expr = sel.targets.get(0).expr();
                    RowContext ctx = new RowContext(table, table.getName(), row);
                    return executor.evalExpr(expr, ctx);
                }
            }
        } catch (Exception e) {
            // fallback: return null
        }
        return null;
    }

    /**
     * PostgreSQL's seed for hash partitioning. Every key is hashed with it, which is what makes
     * the mapping from key to partition the same on every server.
     */
    private static final long HASH_PARTITION_SEED = 0x7A5B22367996DCFDL;

    /**
     * The hash of a whole partition key, as compute_partition_hash_value builds it: each non-NULL
     * column's <em>extended</em> (64-bit) hash folded into a running value, with a NULL column
     * contributing nothing at all. The remainder is then taken over the unsigned result.
     *
     * <p>The 32-bit hash this replaced was a different function over a truncated key, so rows
     * landed in different partitions from PostgreSQL's, and for text every row landed in one.
     */
    static long pgHashRow(Object key) {
        long rowHash = 0L;
        if (key instanceof List) {
            for (Object v : (List<?>) key) {
                if (v != null) rowHash = hashCombine64(rowHash, pgHashValue(v));
            }
        } else if (key != null) {
            rowHash = hashCombine64(rowHash, pgHashValue(key));
        }
        return rowHash;
    }

    /** PostgreSQL's hash_combine64. */
    private static long hashCombine64(long a, long b) {
        return a ^ (b + 0x49a0f4dd15e5a8e3L + (a << 54) + (a >>> 2));
    }

    /**
     * The extended hash of one key value, chosen by its type the way PostgreSQL chooses the
     * operator class's support function: hashint4extended for the narrow integers,
     * hashint8extended for bigint, hashtextextended for text. A type with no port yet is hashed
     * through its text form, which at least routes every value of it consistently.
     */
    private static long pgHashValue(Object v) {
        if (v instanceof Long) return hashInt8Extended((Long) v, HASH_PARTITION_SEED);
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return hashUint32Extended(((Number) v).intValue(), HASH_PARTITION_SEED);
        }
        if (v instanceof Boolean) {
            return hashUint32Extended(((Boolean) v).booleanValue() ? 1 : 0, HASH_PARTITION_SEED);
        }
        if (v instanceof java.math.BigInteger) {
            java.math.BigInteger b = (java.math.BigInteger) v;
            if (b.bitLength() < 64) return hashInt8Extended(b.longValue(), HASH_PARTITION_SEED);
        }
        if (v instanceof String) {
            return hashAnyExtended(((String) v).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    HASH_PARTITION_SEED);
        }
        if (v instanceof byte[]) return hashAnyExtended((byte[]) v, HASH_PARTITION_SEED);
        return hashAnyExtended(String.valueOf(v).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                HASH_PARTITION_SEED);
    }

    private static int rot32(int x, int k) {
        return (x << k) | (x >>> (32 - k));
    }

    /** Jenkins lookup3's mix, over the three words held in {@code s}. */
    private static void lookup3Mix(int[] s) {
        int a = s[0], b = s[1], c = s[2];
        a -= c; a ^= rot32(c, 4);  c += b;
        b -= a; b ^= rot32(a, 6);  a += c;
        c -= b; c ^= rot32(b, 8);  b += a;
        a -= c; a ^= rot32(c, 16); c += b;
        b -= a; b ^= rot32(a, 19); a += c;
        c -= b; c ^= rot32(b, 4);  b += a;
        s[0] = a; s[1] = b; s[2] = c;
    }

    /** Jenkins lookup3's final. */
    private static void lookup3Final(int[] s) {
        int a = s[0], b = s[1], c = s[2];
        c ^= b; c -= rot32(b, 14);
        a ^= c; a -= rot32(c, 11);
        b ^= a; b -= rot32(a, 25);
        c ^= b; c -= rot32(b, 16);
        a ^= c; a -= rot32(c, 4);
        b ^= a; b -= rot32(a, 14);
        c ^= b; c -= rot32(b, 24);
        s[0] = a; s[1] = b; s[2] = c;
    }

    /** PostgreSQL's hash_uint32_extended, which is what hashint4extended is. */
    private static long hashUint32Extended(int k, long seed) {
        int init = 0x9e3779b9 + 4 + 3923095;
        int[] s = new int[] { init, init, init };
        if (seed != 0) {
            s[0] += (int) (seed >>> 32);
            s[1] += (int) seed;
            lookup3Mix(s);
        }
        s[0] += k;
        lookup3Final(s);
        return (((long) s[1]) << 32) | (s[2] & 0xFFFFFFFFL);
    }

    /** PostgreSQL's hashint8extended: the halves are folded together, then hashed as one word. */
    private static long hashInt8Extended(long v, long seed) {
        int lo = (int) v;
        int hi = (int) (v >>> 32);
        lo ^= (v >= 0) ? hi : ~hi;
        return hashUint32Extended(lo, seed);
    }

    /** Four bytes read the way PostgreSQL reads them on a little-endian machine. */
    private static int leWord(byte[] k, int off) {
        return (k[off] & 0xFF)
                | ((k[off + 1] & 0xFF) << 8)
                | ((k[off + 2] & 0xFF) << 16)
                | ((k[off + 3] & 0xFF) << 24);
    }

    /** PostgreSQL's hash_any_extended, which hashtextextended is over the string's bytes. */
    private static long hashAnyExtended(byte[] k, long seed) {
        int init = 0x9e3779b9 + k.length + 3923095;
        int[] s = new int[] { init, init, init };
        if (seed != 0) {
            s[0] += (int) (seed >>> 32);
            s[1] += (int) seed;
            lookup3Mix(s);
        }
        int off = 0;
        int rem = k.length;
        while (rem >= 12) {
            s[0] += leWord(k, off);
            s[1] += leWord(k, off + 4);
            s[2] += leWord(k, off + 8);
            lookup3Mix(s);
            off += 12;
            rem -= 12;
        }
        // The last 11 bytes, folded in the order lookup3 folds them: each case falls into the next.
        switch (rem) {
            case 11: s[2] += (k[off + 10] & 0xFF) << 24;
            case 10: s[2] += (k[off + 9] & 0xFF) << 16;
            case 9:  s[2] += (k[off + 8] & 0xFF) << 8;
            case 8:  s[1] += leWord(k, off + 4); s[0] += leWord(k, off); break;
            case 7:  s[1] += (k[off + 6] & 0xFF) << 16;
            case 6:  s[1] += (k[off + 5] & 0xFF) << 8;
            case 5:  s[1] += (k[off + 4] & 0xFF);
            case 4:  s[0] += leWord(k, off); break;
            case 3:  s[0] += (k[off + 2] & 0xFF) << 16;
            case 2:  s[0] += (k[off + 1] & 0xFF) << 8;
            case 1:  s[0] += (k[off] & 0xFF); break;
            default: break;
        }
        lookup3Final(s);
        return (((long) s[1]) << 32) | (s[2] & 0xFFFFFFFFL);
    }

    /**
     * Collect this table and all its partition tables (recursively) into the list.
     * For non-partitioned tables, just adds the table itself.
     */
    static void collectAllPartitionTables(Table table, List<Table> result) {
        if (table.getPartitions().isEmpty()) {
            result.add(table);
        } else {
            result.add(table); // parent may have own rows too
            for (Table partition : table.getPartitions()) {
                collectAllPartitionTables(partition, result);
            }
        }
    }

    /**
     * Collect this relation and every relation that stores rows for it: its partitions and its
     * inheritance children, recursively. A statement that names a parent reaches all of them —
     * which is what {@link Table#getAllRows} already does on the read side, while the write side
     * followed partitions only and so left every inherited child untouched.
     */
    static void collectRelationAndDescendants(Table table, List<Table> result) {
        // Multiple inheritance can reach one child down two paths; it must be written to once.
        for (Table seen : result) {
            if (seen == table) return;
        }
        result.add(table);
        for (Table partition : table.getPartitions()) {
            collectRelationAndDescendants(partition, result);
        }
        for (Table child : table.getChildren()) {
            collectRelationAndDescendants(child, result);
        }
    }
}
