package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Holds the current row context during query execution.
 * Maps table aliases to their Table + current row data.
 * Supports single-table (UPDATE/DELETE) and multi-table (FROM cross join, JOINs) contexts.
 */
public class RowContext {

        public static final class TableBinding {
        public final Table table;
        public final String alias;
        public final Object[] row;
        public final Table sourceTable;

        public TableBinding(Table table, String alias, Object[] row, Table sourceTable) {
            this.table = table;
            this.alias = alias;
            this.row = row;
            this.sourceTable = sourceTable;
        }

        /** Convenience constructor without sourceTable (defaults to table). */
        public TableBinding(Table table, String alias, Object[] row) {
            this(table, alias, row, table);
        }

        public Table table() { return table; }
        public String alias() { return alias; }
        public Object[] row() { return row; }
        public Table sourceTable() { return sourceTable; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableBinding that = (TableBinding) o;
            return java.util.Objects.equals(table, that.table)
                && java.util.Objects.equals(alias, that.alias)
                && java.util.Arrays.equals(row, that.row)
                && java.util.Objects.equals(sourceTable, that.sourceTable);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(table, alias, java.util.Arrays.hashCode(row), sourceTable);
        }

        @Override
        public String toString() {
            return "TableBinding[table=" + table + ", " + "alias=" + alias + ", " + "row=" + java.util.Arrays.toString(row) + ", " + "sourceTable=" + sourceTable + "]";
        }
    }

    /**
     * One column of what a FROM item exposes: a name and the binding columns it is read from.
     *
     * <p>Nearly every output column is one binding's column. A join written with USING or NATURAL
     * merges the column named on its left with the one on its right into a single output column
     * whose value is whichever of them is not null, and that is the only reason an output column
     * ever has more than one source. Chaining such joins keeps merging: the left of the second
     * {@code USING (id)} is the first join's already-merged {@code id}, so the second merge holds
     * three sources and the whole chain still exposes one {@code id}.
     */
    public static final class OutCol {
        public final String name;
        /** Binding indices to read, in the order the value falls back through them. */
        public final int[] bindings;
        /** The column index within each of those bindings. */
        public final int[] columns;

        public OutCol(String name, int[] bindings, int[] columns) {
            this.name = name;
            this.bindings = bindings;
            this.columns = columns;
        }

        public OutCol(String name, int binding, int column) {
            this(name, new int[]{binding}, new int[]{column});
        }

        /** True when a USING or NATURAL join folded several relations' columns into this one. */
        public boolean merged() { return bindings.length > 1; }

        /** The same column read from a binding list this one has been appended to. */
        public OutCol shift(int delta) {
            if (delta == 0) return this;
            int[] b = new int[bindings.length];
            for (int i = 0; i < b.length; i++) b[i] = bindings[i] + delta;
            return new OutCol(name, b, columns);
        }

        /** The value this column takes in a row: the first source that is not null. */
        public Object valueIn(List<TableBinding> row) {
            for (int i = 0; i < bindings.length; i++) {
                if (bindings[i] >= row.size()) continue;
                Object[] r = row.get(bindings[i]).row();
                if (columns[i] >= r.length) continue;
                Object v = r[columns[i]];
                if (v != null) return v;
            }
            return null;
        }

        @Override
        public String toString() {
            return "OutCol[" + name + " " + java.util.Arrays.toString(bindings)
                    + java.util.Arrays.toString(columns) + "]";
        }
    }

    private final List<TableBinding> bindings;
    /**
     * The columns this row exposes, in order, when a join merged some of them; null when the
     * answer is simply every binding's columns in order. See {@link OutCol}.
     */
    private List<OutCol> outputColumns;
    /** True when this row was produced by a LEFT/RIGHT/FULL JOIN with no match on the outer side. */
    private boolean outerJoinNullPadded;
    /** Column names from USING clauses. These exist in multiple bindings but should not raise ambiguity. */
    private Set<String> usingColumns;
    /**
     * View-column aliasing: maps a view column name (lower-case) to the underlying base-table
     * column name. Set when a DML statement runs through an auto-updatable view that renames
     * columns, so WHERE / RETURNING references to the view's names resolve against the base row.
     */
    private java.util.Map<String, String> columnAliases;

    /** Register the view→base column-name aliasing for this context (see {@link #columnAliases}). */
    public void setColumnAliases(java.util.Map<String, String> columnAliases) {
        this.columnAliases = columnAliases;
    }

    /** Translate a view column name to its base-table name when an aliasing map is present. */
    private String aliasColumn(String columnName) {
        if (columnAliases == null || columnName == null) return columnName;
        String mapped = columnAliases.get(columnName.toLowerCase());
        return mapped != null ? mapped : columnName;
    }
    /**
     * Values already settled for particular AST nodes, keyed by identity rather than by
     * structural equality — the same query text written twice is two distinct node instances,
     * and only the one that was bound may read the bound value. Evaluation returns the bound
     * value instead of computing the node again. Two callers need this:
     *
     * <ul>
     *   <li>a set-returning function nested inside a larger SELECT-list expression (e.g.
     *       {@code day_start + interval '1h' * generate_series(0,23,2)}): the SRF is evaluated
     *       once per row to get its element list, and the owning expression is re-evaluated once
     *       per element with the call bound to that element. See
     *       {@code SelectExecutor.findSrfCall};</li>
     *   <li>the inputs of a window function over a grouped query ({@code sum(sum(v)) OVER ()},
     *       {@code rank() OVER (ORDER BY sum(v))}): those are values of the grouped row, computed
     *       once per group before the window runs over the groups. See
     *       {@code SelectAggregateEvaluator.GroupedWindowPass}.</li>
     * </ul>
     */
    private java.util.Map<com.memgres.engine.parser.ast.Expression, Object> boundValues;

    /** Binds a value to substitute for {@code node} the next time it is evaluated in this context. */
    public void setBoundValue(com.memgres.engine.parser.ast.Expression node, Object value) {
        if (boundValues == null) boundValues = new java.util.IdentityHashMap<>();
        boundValues.put(node, value);
    }

    /** Removes any substitution bound for {@code node} (call after re-evaluating the owning expr). */
    public void clearBoundValue(com.memgres.engine.parser.ast.Expression node) {
        if (boundValues != null) boundValues.remove(node);
    }

    /** Returns true if a substitution is currently bound for {@code node} (may map to a null value). */
    public boolean hasBoundValue(com.memgres.engine.parser.ast.Expression node) {
        return boundValues != null && boundValues.containsKey(node);
    }

    /** Returns the substituted value for {@code node}; only valid when {@link #hasBoundValue} is true. */
    public Object getBoundValue(com.memgres.engine.parser.ast.Expression node) {
        return boundValues == null ? null : boundValues.get(node);
    }

    /** Single-table context (used by UPDATE, DELETE, triggers). */
    public RowContext(Table table, String alias, Object[] row) {
        this.bindings = Cols.listOf(new TableBinding(table, alias, row));
    }

    /** Multi-table context (used by SELECT with multiple FROM tables / JOINs). */
    public RowContext(List<TableBinding> bindings) {
        this.bindings = bindings;
    }

    public boolean isOuterJoinNullPadded() {
        return outerJoinNullPadded;
    }

    public void setOuterJoinNullPadded(boolean outerJoinNullPadded) {
        this.outerJoinNullPadded = outerJoinNullPadded;
    }

    /** The columns this row exposes, in order, or null when that is simply all of them. */
    public List<OutCol> getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(List<OutCol> outputColumns) {
        this.outputColumns = outputColumns;
    }

    /** The columns this row exposes, filled in when no join has changed them. */
    public List<OutCol> outputColumnsOrDefault() {
        return outputColumns != null ? outputColumns : defaultOutput(bindings);
    }

    /** What a list of bindings exposes when no join merged anything: every column, in order. */
    public static List<OutCol> defaultOutput(List<TableBinding> bindings) {
        List<OutCol> out = new ArrayList<>();
        for (int bi = 0; bi < bindings.size(); bi++) {
            List<Column> cols = bindings.get(bi).table().getColumns();
            for (int ci = 0; ci < cols.size(); ci++) {
                out.add(new OutCol(cols.get(ci).getName(), bi, ci));
            }
        }
        return out;
    }

    public Set<String> getUsingColumns() {
        return usingColumns;
    }

    public void setUsingColumns(Set<String> usingColumns) {
        this.usingColumns = usingColumns;
    }

    public List<TableBinding> getBindings() {
        return bindings;
    }

    /**
     * A second context over the same row, whose bound values can be set independently of this
     * one's. Used where one input row has to stand for several output rows -- a set-returning
     * call in GROUP BY or ORDER BY produces one row per element and each needs its own binding.
     */
    public RowContext copy() {
        RowContext copy = new RowContext(bindings);
        copy.outerJoinNullPadded = outerJoinNullPadded;
        copy.usingColumns = usingColumns;
        copy.outputColumns = outputColumns;
        copy.columnAliases = columnAliases;
        if (boundValues != null) {
            copy.boundValues = new java.util.IdentityHashMap<>(boundValues);
        }
        return copy;
    }

    /**
     * Find the binding for a given table name or alias. Follows PG scoping: an alias
     * hides the table's real name, so "SELECT pg_type.x FROM pg_type te" must NOT bind
     * the inner scan — the qualified reference either correlates to an outer query
     * level or errors. The underlying table name therefore only matches bindings that
     * carry no alias (or whose alias is the table name itself).
     */
    public TableBinding getBinding(String qualifier) {
        for (TableBinding b : bindings) {
            if (b.alias() != null) {
                if (b.alias().equalsIgnoreCase(qualifier)) return b;
            } else if (b.table().getName().equalsIgnoreCase(qualifier)) {
                return b;
            }
        }
        return null;
    }

    /**
     * Resolve a column value. Handles both qualified (table.col) and unqualified (col) references.
     * For unqualified references, throws on ambiguity (column exists in multiple tables).
     */
    public Object resolveColumn(String tableQualifier, String columnName) {
        // Handle tableoid pseudo-column
        if ("tableoid".equalsIgnoreCase(columnName)) {
            return resolveTableoid(tableQualifier);
        }
        // Handle system columns: ctid, xmin, xmax, cmin, cmax
        String lcCol = columnName.toLowerCase();
        if (lcCol.equals("ctid") || lcCol.equals("xmin") || lcCol.equals("xmax")
                || lcCol.equals("cmin") || lcCol.equals("cmax")) {
            return resolveSystemColumn(tableQualifier, lcCol);
        }
        // Translate view column names to base-table names for renamed-column view DML.
        columnName = aliasColumn(columnName);

        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            if (b == null) {
                // Check if the qualifier matches a table whose real name is hidden by an alias
                for (TableBinding tb : bindings) {
                    if (tb.alias() != null && !tb.alias().equalsIgnoreCase(tb.table().getName())
                            && tb.table().getName().equalsIgnoreCase(tableQualifier)) {
                        MemgresException ex = new MemgresException(
                                "invalid reference to FROM-clause entry for table \"" + tableQualifier + "\"", "42P01");
                        ex.setHint("Perhaps you meant to reference the table alias \"" + tb.alias() + "\".");
                        throw ex;
                    }
                }
                throw new MemgresException("missing FROM-clause entry for table \"" + tableQualifier + "\"", "42P01");
            }
            int idx = b.table().getColumnIndex(columnName);
            if (idx < 0) {
                MemgresException ex = new MemgresException("column " + tableQualifier + "." + columnName + " does not exist", "42703");
                String hint = suggestClosestColumn(columnName, b.table());
                if (hint != null) ex.setHint(hint);
                throw ex;
            }
            return b.row()[idx];
        }

        // Unqualified. When a join merged columns, what the name may resolve to is the join's
        // output rather than the relations behind it: one merged column however many relations
        // fed it, and still ambiguous when two output columns answer to the name.
        if (outputColumns != null) {
            OutCol hit = null;
            int matches = 0;
            for (OutCol oc : outputColumns) {
                if (oc.name.equalsIgnoreCase(columnName)) {
                    matches++;
                    if (hit == null) hit = oc;
                }
            }
            if (matches > 1) {
                throw new MemgresException("column reference \"" + columnName + "\" is ambiguous", "42702");
            }
            if (matches == 1) return hit.valueIn(bindings);
        }

        // Unqualified, search all bindings
        Object result = null;
        boolean found = false;
        boolean isUsingCol = usingColumns != null && usingColumns.contains(columnName.toLowerCase());
        for (TableBinding b : bindings) {
            int idx = b.table().getColumnIndex(columnName);
            if (idx >= 0) {
                if (found && !isUsingCol) {
                    throw new MemgresException("column reference \"" + columnName + "\" is ambiguous", "42702");
                }
                if (!found) {
                    result = b.row()[idx];
                } else if (isUsingCol && result == null) {
                    // USING column COALESCE: if left side is NULL, use right side
                    result = b.row()[idx];
                }
                found = true;
            }
        }
        if (!found) {
            MemgresException ex = new MemgresException("column \"" + columnName + "\" does not exist", "42703");
            // Try to suggest a close match from any binding
            for (TableBinding b : bindings) {
                String hint = suggestClosestColumn(columnName, b.table());
                if (hint != null) { ex.setHint(hint); break; }
            }
            throw ex;
        }
        return result;
    }

    /**
     * Resolve the tableoid pseudo-column for a binding.
     * Returns a placeholder integer that will be resolved via SystemCatalog OID lookup.
     * The sourceTable is the actual table that stores the row (partition for partitioned tables).
     */
    private Object resolveTableoid(String tableQualifier) {
        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            if (b == null) {
                throw new MemgresException("missing FROM-clause entry for table \"" + tableQualifier + "\"", "42P01");
            }
            // Return the source table name so it can be resolved to an OID
            return new TableoidRef(b.sourceTable());
        }
        // Unqualified, return from first binding
        if (!bindings.isEmpty()) {
            return new TableoidRef(bindings.get(0).sourceTable());
        }
        throw new MemgresException("column \"tableoid\" does not exist", "42703");
    }

    /**
     * Resolve system columns (ctid, xmin, xmax, cmin, cmax) for a row.
     */
    private Object resolveSystemColumn(String tableQualifier, String colName) {
        TableBinding b;
        if (tableQualifier != null) {
            b = getBinding(tableQualifier);
            if (b == null) {
                throw new MemgresException("missing FROM-clause entry for table \"" + tableQualifier + "\"", "42P01");
            }
        } else {
            if (bindings.isEmpty()) {
                throw new MemgresException("column \"" + colName + "\" does not exist", "42703");
            }
            b = bindings.get(0);
        }
        Table table = b.sourceTable();
        Object[] row = b.row();
        if (colName.equals("ctid")) {
            // Return a SystemColumnRef so ExprEvaluator can compute with metadata
            return new SystemColumnRef(table, row, "ctid");
        }
        // xmin, xmax, cmin, cmax: look up from row metadata
        return new SystemColumnRef(table, row, colName);
    }

    /** Marker for deferred system column resolution (xmin/xmax/cmin/cmax). */
    public static final class SystemColumnRef {
        public final Table table;
        public final Object[] row;
        public final String column;

        public SystemColumnRef(Table table, Object[] row, String column) {
            this.table = table;
            this.row = row;
            this.column = column;
        }
    }

    /**
     * A marker object holding a reference to the source table for tableoid resolution.
     * The AstExecutor/CastEvaluator will resolve this to the actual OID integer.
     */
        public static final class TableoidRef {
        public final Table table;

        public TableoidRef(Table table) {
            this.table = table;
        }

        public Table table() { return table; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableoidRef that = (TableoidRef) o;
            return java.util.Objects.equals(table, that.table);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(table);
        }

        @Override
        public String toString() {
            return "TableoidRef[table=" + table + "]";
        }
    }

    /**
     * Resolve column metadata (for type inference).
     */
    public Column resolveColumnDef(String tableQualifier, String columnName) {
        // tableoid is a pseudo-column of type oid (integer)
        if ("tableoid".equalsIgnoreCase(columnName)) {
            return new Column("tableoid", DataType.INTEGER, false, false, null);
        }
        // System columns
        String lc = columnName.toLowerCase();
        if (lc.equals("ctid")) return new Column("ctid", DataType.TEXT, false, false, null);
        if (lc.equals("xmin") || lc.equals("xmax")) return new Column(lc, DataType.BIGINT, false, false, null);
        if (lc.equals("cmin") || lc.equals("cmax")) return new Column(lc, DataType.INTEGER, false, false, null);
        columnName = aliasColumn(columnName);

        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            if (b == null) return null;
            int idx = b.table().getColumnIndex(columnName);
            return idx >= 0 ? b.table().getColumns().get(idx) : null;
        }

        if (outputColumns != null) {
            for (OutCol oc : outputColumns) {
                if (oc.name.equalsIgnoreCase(columnName) && oc.bindings[0] < bindings.size()) {
                    return bindings.get(oc.bindings[0]).table().getColumns().get(oc.columns[0]);
                }
            }
        }
        for (TableBinding b : bindings) {
            int idx = b.table().getColumnIndex(columnName);
            if (idx >= 0) {
                return b.table().getColumns().get(idx);
            }
        }
        return null;
    }

    /**
     * Check if a column exists in any binding (for checking column name validity without throwing).
     */
    public boolean hasColumn(String columnName) {
        if ("tableoid".equalsIgnoreCase(columnName)) return true;
        columnName = aliasColumn(columnName);
        for (TableBinding b : bindings) {
            if (b.table().getColumnIndex(columnName) >= 0) return true;
        }
        return false;
    }

    /**
     * Create a new RowContext that merges this context's bindings with another's.
     */
    public RowContext merge(RowContext other) {
        List<TableBinding> merged = new ArrayList<>(this.bindings);
        merged.addAll(other.bindings);
        RowContext result = new RowContext(merged);
        // Preserve view-column aliasing from either side (only the view side carries it).
        result.columnAliases = this.columnAliases != null ? this.columnAliases : other.columnAliases;
        result.outputColumns = concatOutput(this, other);
        return result;
    }

    /**
     * The columns two sides put side by side expose together, kept only when one of them has
     * something to say — a plain pairing of relations is described well enough by its bindings.
     */
    public static List<OutCol> concatOutput(RowContext left, RowContext right) {
        if (left.outputColumns == null && right.outputColumns == null) return null;
        List<OutCol> out = new ArrayList<>(left.outputColumnsOrDefault());
        int offset = left.bindings.size();
        for (OutCol oc : right.outputColumnsOrDefault()) out.add(oc.shift(offset));
        return out;
    }

    /**
     * Suggest the closest matching column name from a table for a typo hint.
     * Uses Levenshtein edit distance. Returns null if no close match found.
     */
    static String suggestClosestColumn(String typo, Table table) {
        if (table == null || typo == null) return null;
        String bestName = null;
        int bestDist = Integer.MAX_VALUE;
        String lowerTypo = typo.toLowerCase();
        for (Column col : table.getColumns()) {
            String colName = col.getName().toLowerCase();
            int dist = editDistance(lowerTypo, colName);
            if (dist < bestDist) {
                bestDist = dist;
                bestName = col.getName();
            }
        }
        // Only suggest if the edit distance is small relative to the name length
        if (bestName != null && bestDist <= Math.max(1, typo.length() / 2)) {
            return "Perhaps you meant to reference the column \"" + bestName + "\".";
        }
        return null;
    }

    /** Compute Levenshtein edit distance between two strings. */
    private static int editDistance(String a, String b) {
        int m = a.length(), n = b.length();
        int[] prev = new int[n + 1];
        int[] curr = new int[n + 1];
        for (int j = 0; j <= n; j++) prev[j] = j;
        for (int i = 1; i <= m; i++) {
            curr[0] = i;
            for (int j = 1; j <= n; j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] tmp = prev; prev = curr; curr = tmp;
        }
        return prev[n];
    }
}
