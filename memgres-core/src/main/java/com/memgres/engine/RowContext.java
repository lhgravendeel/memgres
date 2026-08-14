package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.Collections;
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
        /**
         * The row as the relation that stores it holds it, which is the tuple its system columns
         * belong to. A row read through a partitioned or an inheritance parent is rearranged to
         * that parent's columns, and the rearranged copy lives nowhere: ctid, xmin and the rest
         * are answered from the place the row occupies in the relation below.
         */
        public final Object[] storedRow;

        public TableBinding(Table table, String alias, Object[] row, Table sourceTable) {
            this(table, alias, row, sourceTable, row);
        }

        public TableBinding(Table table, String alias, Object[] row, Table sourceTable,
                            Object[] storedRow) {
            this.table = table;
            this.alias = alias;
            this.row = row;
            this.sourceTable = sourceTable;
            this.storedRow = storedRow;
        }

        /** Convenience constructor without sourceTable (defaults to table). */
        public TableBinding(Table table, String alias, Object[] row) {
            this(table, alias, row, table);
        }

        public Table table() { return table; }
        public String alias() { return alias; }
        public Object[] row() { return row; }
        public Table sourceTable() { return sourceTable; }
        public Object[] storedRow() { return storedRow; }

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
        /**
         * The type a merged column carries when its two sides did not already share one, or null.
         *
         * <p>A USING or NATURAL join's merged column is not either side's column: PostgreSQL
         * resolves one type both sides can be read as and the merged column is that type, so
         * {@code int JOIN bigint USING (k)} exposes a bigint. Taking the left side's type instead
         * described the column as int4 and left the value whichever raw one came first.
         */
        public final DataType type;

        public OutCol(String name, int[] bindings, int[] columns) {
            this(name, bindings, columns, null);
        }

        public OutCol(String name, int[] bindings, int[] columns, DataType type) {
            this.name = name;
            this.bindings = bindings;
            this.columns = columns;
            this.type = type;
        }

        public OutCol(String name, int binding, int column) {
            this(name, new int[]{binding}, new int[]{column});
        }

        /** True when a USING or NATURAL join folded several relations' columns into this one. */
        public boolean merged() { return bindings.length > 1; }

        /** The same column, read as {@code t}. */
        public OutCol withType(DataType t) {
            return new OutCol(name, bindings, columns, t);
        }

        /** The same column read from a binding list this one has been appended to. */
        public OutCol shift(int delta) {
            if (delta == 0) return this;
            int[] b = new int[bindings.length];
            for (int i = 0; i < b.length; i++) b[i] = bindings[i] + delta;
            return new OutCol(name, b, columns, type);
        }

        /** The value this column takes in a row: the first source that is not null. */
        public Object valueIn(List<TableBinding> row) {
            for (int i = 0; i < bindings.length; i++) {
                if (bindings[i] >= row.size()) continue;
                Object[] r = row.get(bindings[i]).row();
                if (columns[i] >= r.length) continue;
                Object v = r[columns[i]];
                if (v != null) return type == null ? v : TypeCoercion.coerce(v, type);
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
     * Relations the FROM clause holds but does not answer to — the ones under an aliased
     * parenthesized join. Naming one is a reference that cannot reach, not a missing entry.
     */
    private Set<String> coveredNames;
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
        TableBinding found = null;
        for (TableBinding b : bindings) {
            boolean matches = b.alias() != null
                    ? b.alias().equalsIgnoreCase(qualifier)
                    : b.table().getName().equalsIgnoreCase(qualifier);
            if (!matches) continue;
            // Two relations of one name from two schemas may both stand in a FROM clause --
            // FROM s1.t, s2.t is legal, because either can still be reached by writing its
            // schema. Written bare, the name reaches both, and PostgreSQL says so rather than
            // answering from whichever it finds first, which is what taking the first match did.
            if (found != null && found != b) {
                throw new MemgresException(
                        "table reference \"" + qualifier + "\" is ambiguous", "42P09");
            }
            found = b;
        }
        return found;
    }

    /**
     * Every binding {@code qualifier} names, without judging whether naming several is a fault --
     * for the callers that have already pinned one down by other means and only need to know
     * which, or how many, the bare name would have reached.
     */
    public List<TableBinding> bindingsNamed(String qualifier) {
        List<TableBinding> named = new ArrayList<>();
        for (TableBinding b : bindings) {
            boolean matches = b.alias() != null
                    ? b.alias().equalsIgnoreCase(qualifier)
                    : b.table().getName().equalsIgnoreCase(qualifier);
            if (matches) named.add(b);
        }
        return named;
    }

    /** Records the relations this row's FROM clause covers over. See {@code coveredNames}. */
    public void setCoveredNames(Set<String> names) { this.coveredNames = names; }

    /**
     * What a qualifier no binding answers to is: a relation the query does not have, or one it has
     * written down under a clause that renamed it. PostgreSQL words the two differently, and
     * calling the second missing sent the reader looking for something they had already written.
     */
    private MemgresException noSuchFromEntry(String qualifier) {
        if (coveredNames == null || !coveredNames.contains(qualifier.toLowerCase())) {
            return new MemgresException(
                    "missing FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        }
        MemgresException e = new MemgresException(
                "invalid reference to FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        e.setDetail("There is an entry for table \"" + qualifier
                + "\", but it cannot be referenced from this part of the query.");
        return e;
    }

    /**
     * Resolve a column value. Handles both qualified (table.col) and unqualified (col) references.
     * For unqualified references, throws on ambiguity (column exists in multiple tables).
     */
    public Object resolveColumn(String tableQualifier, String columnName) {
        // Handle system columns: tableoid, ctid, xmin, xmax, cmin, cmax. A FROM item may expose a
        // column of its own under one of these names -- a subquery, a CTE or a view that projects
        // ctid has an ordinary column called ctid -- and PostgreSQL resolves the name against the
        // columns the item exposes before it looks for a system column. Reading the derived
        // relation's own position instead renumbered every row it carried up.
        String lcCol = columnName.toLowerCase();
        boolean systemName = lcCol.equals("tableoid") || lcCol.equals("ctid") || lcCol.equals("xmin")
                || lcCol.equals("xmax") || lcCol.equals("cmin") || lcCol.equals("cmax");
        if (systemName && !aBindingDeclares(tableQualifier, columnName)) {
            if (lcCol.equals("tableoid")) return resolveTableoid(tableQualifier);
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
                throw noSuchFromEntry(tableQualifier);
            }
            int idx = b.table().getColumnIndex(columnName);
            if (idx < 0) {
                MemgresException ex = new MemgresException("column " + tableQualifier + "." + columnName + " does not exist", "42703");
                String hint = suggestClosestColumn(columnName, Collections.singletonList(b));
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
                if (oc.name.equals(columnName)) {
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
            String hint = suggestClosestColumn(columnName, bindings);
            if (hint != null) ex.setHint(hint);
            throw ex;
        }
        return result;
    }

    /** Whether a FROM item this name may reach declares a column of its own under that name. */
    private boolean aBindingDeclares(String tableQualifier, String columnName) {
        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            return b != null && b.table().getColumnIndex(columnName) >= 0;
        }
        for (TableBinding b : bindings) {
            if (b.table().getColumnIndex(columnName) >= 0) return true;
        }
        return false;
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
        Object[] row = b.storedRow();
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
                    Column source = bindings.get(oc.bindings[0]).table().getColumns().get(oc.columns[0]);
                    // A merged column is declared with the type the join resolved for it, not with
                    // either side's -- which is what pg_typeof is being asked about.
                    return oc.type == null ? source
                            : new Column(source.getName(), oc.type, source.isNullable(), false, null);
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
        String name = closestColumn(typo, table);
        return name == null ? null
                : "Perhaps you meant to reference the column \"" + name + "\".";
    }

    /**
     * The same, for a definition stored against one relation — a CHECK, a generation expression,
     * an index predicate. PostgreSQL writes the suggestion qualified there too, with the name of
     * the relation the definition belongs to, because that is the only relation in scope.
     */
    static String suggestClosestColumnOn(String typo, Table table) {
        String name = table == null ? null : closestColumn(typo, table);
        return name == null ? null : "Perhaps you meant to reference the column \""
                + table.getName() + "." + name + "\".";
    }

    /**
     * The hint for a name no relation in scope answers to, naming every relation that has a near
     * miss and qualifying each with the name that relation is known by.
     *
     * <p>PostgreSQL always writes the suggestion qualified — {@code "a.t"}, not {@code "t"} — since
     * an unqualified suggestion for a query with two relations would be as ambiguous as the name
     * that failed, and it offers one per relation. Suggesting the bare column name told the reader
     * to write what they had just written.
     */
    static String suggestClosestColumn(String typo, List<TableBinding> bindings) {
        List<String> suggestions = new ArrayList<>();
        for (TableBinding b : bindings) {
            String column = closestColumn(typo, b.table());
            if (column == null) continue;
            String relation = b.alias() != null ? b.alias() : b.table().getName();
            String qualified = "\"" + relation + "." + column + "\"";
            if (!suggestions.contains(qualified)) suggestions.add(qualified);
        }
        if (suggestions.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("Perhaps you meant to reference the column ");
        for (int i = 0; i < suggestions.size(); i++) {
            if (i > 0) sb.append(i == suggestions.size() - 1 ? " or the column " : ", the column ");
            sb.append(suggestions.get(i));
        }
        return sb.append('.').toString();
    }

    /**
     * The hint for a name nothing in a range table answers to, in the order the range table holds
     * its relations and with a relation it holds twice offering its column twice.
     *
     * <p>PostgreSQL keeps the closest spelling it found anywhere and offers it while one or two
     * columns are that close. Three equally close and it says nothing at all rather than list
     * them, which is the difference between a suggestion and a catalogue.
     */
    static String suggestClosestColumnAcross(String typo, List<TableBinding> bindings) {
        if (typo == null) return null;
        List<String> closest = new ArrayList<>();
        int shortest = Integer.MAX_VALUE;
        for (TableBinding b : bindings) {
            if (b.table() == null) continue;
            String relation = b.alias() != null ? b.alias() : b.table().getName();
            for (Column col : b.table().getColumns()) {
                int distance = editDistance(typo, col.getName());
                if (distance > FURTHEST_SUGGESTION || distance > typo.length() / 2) continue;
                if (distance > shortest) continue;
                if (distance < shortest) {
                    shortest = distance;
                    closest.clear();
                }
                closest.add("\"" + relation + "." + col.getName() + "\"");
            }
        }
        if (closest.isEmpty() || closest.size() > 2) return null;
        StringBuilder sb = new StringBuilder("Perhaps you meant to reference the column ");
        sb.append(closest.get(0));
        if (closest.size() > 1) sb.append(" or the column ").append(closest.get(1));
        return sb.append('.').toString();
    }

    /**
     * How far apart two names may be spelled before a suggestion stops being one. PostgreSQL keeps
     * a candidate only while it is within three edits of what was written, so a name that shares a
     * long tail with the written one — {@code op_bytes} against {@code read_bytes} — is no
     * suggestion however much of it matches.
     */
    private static final int FURTHEST_SUGGESTION = 3;

    /**
     * The column of {@code table} closest to a name it does not have, or null when none is near.
     *
     * <p>Two things decide it, and both are measured against the name as written. The comparison is
     * of the spellings themselves, so a column named {@code MiXeD} is not a near miss for
     * {@code mixed}: four of its five letters are cased differently and that is four edits away.
     * And the distance allowed grows with what was written rather than with what was found, so a
     * one-letter name has no near misses at all while {@code abcdefg} still reaches {@code abcd}.
     */
    private static String closestColumn(String typo, Table table) {
        if (table == null || typo == null) return null;
        String bestName = null;
        int bestDist = Integer.MAX_VALUE;
        for (Column col : table.getColumns()) {
            int dist = editDistance(typo, col.getName());
            if (dist < bestDist) {
                bestDist = dist;
                bestName = col.getName();
            }
        }
        if (bestName == null || bestDist > FURTHEST_SUGGESTION) return null;
        return bestDist <= typo.length() / 2 ? bestName : null;
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
