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

    private final List<TableBinding> bindings;
    /** True when this row was produced by a LEFT/RIGHT/FULL JOIN with no match on the outer side. */
    private boolean outerJoinNullPadded;
    /** Column names from USING clauses. These exist in multiple bindings but should not raise ambiguity. */
    private Set<String> usingColumns;

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
     * Find the binding for a given table name or alias.
     */
    public TableBinding getBinding(String qualifier) {
        for (TableBinding b : bindings) {
            if ((b.alias() != null && b.alias().equalsIgnoreCase(qualifier))
                    || b.table().getName().equalsIgnoreCase(qualifier)) {
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

        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            if (b == null) {
                throw new MemgresException("missing FROM-clause entry for table \"" + tableQualifier + "\"", "42P01");
            }
            int idx = b.table().getColumnIndex(columnName);
            if (idx < 0) {
                throw new MemgresException("column " + tableQualifier + "." + columnName + " does not exist", "42703");
            }
            return b.row()[idx];
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
                }
                found = true;
            }
        }
        if (!found) {
            throw new MemgresException("column \"" + columnName + "\" does not exist", "42703");
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

        if (tableQualifier != null) {
            TableBinding b = getBinding(tableQualifier);
            if (b == null) return null;
            int idx = b.table().getColumnIndex(columnName);
            return idx >= 0 ? b.table().getColumns().get(idx) : null;
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
        return new RowContext(merged);
    }
}
