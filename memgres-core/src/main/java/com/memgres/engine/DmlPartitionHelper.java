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

    /** Route an INSERT row to the correct partition, or return the table itself if not partitioned. */
    Table routeToPartition(Table table, Object[] row) {
        if (table.getPartitionStrategy() == null) return table;

        String partCol = table.getPartitionColumn();
        if (partCol == null) return table;
        Object value;
        if (partCol.contains("(")) {
            // Expression-based partition key (e.g., "(lower(s))")
            // Build a row context and evaluate the expression
            value = evaluatePartitionExpression(table, partCol, row);
        } else if (partCol.contains(",")) {
            // Multi-column partition key: build a tuple in key order
            String[] keyCols = partCol.split(",");
            List<Object> tuple = new java.util.ArrayList<>(keyCols.length);
            for (String keyCol : keyCols) {
                int colIdx = table.getColumnIndex(keyCol.trim());
                if (colIdx < 0) return table;
                tuple.add(row[colIdx]);
            }
            value = tuple;
        } else {
            int colIdx = table.getColumnIndex(partCol);
            if (colIdx < 0) return table;
            value = row[colIdx];
        }

        Table matched = null;
        for (Table partition : table.getPartitions()) {
            switch (table.getPartitionStrategy().toUpperCase()) {
                case "RANGE": {
                    // NULL keys never match a range partition (they go to DEFAULT or error)
                    if (!containsNull(value)
                            && partition.getPartitionLower() != null && partition.getPartitionUpper() != null) {
                        if (compareToBound(value, partition.getPartitionLower()) >= 0
                                && compareToBound(value, partition.getPartitionUpper()) < 0) {
                            matched = partition;
                        }
                    }
                    break;
                }
                case "LIST": {
                    if (partition.getPartitionValues() != null) {
                        for (Object pv : partition.getPartitionValues()) {
                            boolean eq;
                            if (value == null || pv == null) {
                                // SQL NULL routes to a LIST partition declaring NULL in its IN-list
                                eq = value == null && pv == null;
                            } else {
                                eq = executor.compareValues(value, pv) == 0;
                            }
                            if (eq) { matched = partition; break; }
                        }
                    }
                    break;
                }
                case "HASH": {
                    if (partition.getPartitionModulus() != null) {
                        int hash = value == null ? 0 : pgHashPartition(value);
                        int remainder = ((hash % partition.getPartitionModulus()) + partition.getPartitionModulus()) % partition.getPartitionModulus();
                        if (remainder == partition.getPartitionRemainder()) {
                            matched = partition;
                        }
                    }
                    break;
                }
            }
            if (matched != null) break;
        }
        // Fall back to DEFAULT partition
        if (matched == null) {
            for (Table partition : table.getPartitions()) {
                if (partition.isDefaultPartition()) { matched = partition; break; }
            }
        }
        if (matched == null) {
            throw new MemgresException("no partition of relation \"" + table.getName() + "\" found for row", "23514");
        }
        // Recurse for multi-level partitioning (sub-partitions)
        if (matched.getPartitionStrategy() != null && !matched.getPartitions().isEmpty()) {
            return routeToPartition(matched, row);
        }
        return matched;
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
     * PG-compatible hash for partition routing.
     * Uses the same algorithm as PG's hash_combine / hashint4 / hash_uint32.
     */
    static int pgHashPartition(Object value) {
        // PG uses hashint4 for integer keys, which is hash_uint32(datum).
        // hash_uint32 is Thomas Wang's integer hash function.
        int v;
        if (value instanceof Number) {
            Number n = (Number) value;
            v = n.intValue();
        } else {
            v = value.hashCode();
        }
        // Thomas Wang's hash32shift, the function PostgreSQL uses internally
        v = ~v + (v << 21);
        v = v ^ (v >>> 24);
        v = (v + (v << 3)) + (v << 8);
        v = v ^ (v >>> 14);
        v = (v + (v << 2)) + (v << 4);
        v = v ^ (v >>> 28);
        v = v + (v << 31);
        // PG uses the top bits for partition mapping; we just need absolute value
        return Math.abs(v);
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
}
