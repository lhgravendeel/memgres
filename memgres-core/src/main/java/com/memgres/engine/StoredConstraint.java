package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.Expression;

import java.util.List;

/**
 * Runtime representation of a table constraint (PK, UNIQUE, CHECK, FK).
 */
public class StoredConstraint {

    public enum Type { PRIMARY_KEY, UNIQUE, CHECK, FOREIGN_KEY, EXCLUDE }
    public enum FkAction { NO_ACTION, RESTRICT, CASCADE, SET_NULL, SET_DEFAULT }

    /** An element of an EXCLUDE constraint: column + operator. */
        public static final class ExcludeElement {
        public final String column;
        public final String operator;

        public ExcludeElement(String column, String operator) {
            this.column = column;
            this.operator = operator;
        }

        public String column() { return column; }
        public String operator() { return operator; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExcludeElement that = (ExcludeElement) o;
            return java.util.Objects.equals(column, that.column)
                && java.util.Objects.equals(operator, that.operator);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(column, operator);
        }

        @Override
        public String toString() {
            return "ExcludeElement[column=" + column + ", " + "operator=" + operator + "]";
        }
    }
    private List<ExcludeElement> excludeElements;

    private String name;
    private final Type type;
    // Not final: rewritten in place by renameColumn()/renameReferencedColumn() when a column
    // they reference is renamed via ALTER TABLE ... RENAME COLUMN.
    private List<String> columns;
    private Expression checkExpr;
    private final String referencesTable;
    private String referencesSchema; // schema of the referenced table (null = resolve via search_path)
    private List<String> referencesColumns;
    private final FkAction onDelete;
    private final FkAction onUpdate;
    private boolean nullsNotDistinct;
    private boolean deferrable;
    private boolean initiallyDeferred;
    private boolean notEnforced; // PG 18: NOT ENFORCED constraints are stored but not validated
    private boolean noInherit; // CHECK ... NO INHERIT: constraint not inherited by child tables
    private boolean convalidated = true; // pg_constraint.convalidated: false when added with NOT VALID
    private boolean fromIndex; // true if this constraint was created via CREATE UNIQUE INDEX (not ADD CONSTRAINT)
    private boolean promotedFromIndex; // true if created via ADD CONSTRAINT ... UNIQUE USING INDEX
    private String matchType; // FK match type: null/"SIMPLE"/"FULL"/"PARTIAL"
    private Expression whereExpr; // partial index predicate
    private List<Expression> expressionColumns; // parsed expressions for expression-based index columns
    private List<String> onDeleteSetNullColumns; // FK SET NULL column list (subset of FK columns to null)
    private List<String> onUpdateSetNullColumns; // FK SET NULL column list for ON UPDATE

    public StoredConstraint(String name, Type type, List<String> columns,
                            Expression checkExpr,
                            String referencesTable, List<String> referencesColumns,
                            FkAction onDelete, FkAction onUpdate) {
        this.name = name;
        this.type = type;
        this.columns = columns != null ? Cols.listCopyOf(columns) : Cols.listOf();
        this.checkExpr = checkExpr;
        this.referencesTable = referencesTable;
        this.referencesColumns = referencesColumns != null ? Cols.listCopyOf(referencesColumns) : Cols.listOf();
        this.onDelete = onDelete != null ? onDelete : FkAction.NO_ACTION;
        this.onUpdate = onUpdate != null ? onUpdate : FkAction.NO_ACTION;
    }

    public static StoredConstraint primaryKey(String name, List<String> columns) {
        return new StoredConstraint(name, Type.PRIMARY_KEY, columns, null, null, null, null, null);
    }

    public static StoredConstraint unique(String name, List<String> columns) {
        return new StoredConstraint(name, Type.UNIQUE, columns, null, null, null, null, null);
    }

    public static StoredConstraint check(String name, Expression checkExpr) {
        return new StoredConstraint(name, Type.CHECK, null, checkExpr, null, null, null, null);
    }

    public static StoredConstraint foreignKey(String name, List<String> columns,
                                              String refTable, List<String> refColumns,
                                              FkAction onDelete, FkAction onUpdate) {
        return new StoredConstraint(name, Type.FOREIGN_KEY, columns, null, refTable, refColumns, onDelete, onUpdate);
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Type getType() { return type; }
    public List<String> getColumns() { return columns; }
    public Expression getCheckExpr() { return checkExpr; }
    public String getReferencesTable() { return referencesTable; }
    public String getReferencesSchema() { return referencesSchema; }
    public void setReferencesSchema(String schema) { this.referencesSchema = schema; }
    public List<String> getReferencesColumns() { return referencesColumns; }
    public FkAction getOnDelete() { return onDelete; }
    public FkAction getOnUpdate() { return onUpdate; }
    public boolean isNullsNotDistinct() { return nullsNotDistinct; }
    public void setNullsNotDistinct(boolean nullsNotDistinct) { this.nullsNotDistinct = nullsNotDistinct; }
    public List<ExcludeElement> getExcludeElements() { return excludeElements; }
    public void setExcludeElements(List<ExcludeElement> elements) { this.excludeElements = elements; }
    public Expression getWhereExpr() { return whereExpr; }
    public void setWhereExpr(Expression whereExpr) { this.whereExpr = whereExpr; }
    public List<Expression> getExpressionColumns() { return expressionColumns; }
    public void setExpressionColumns(List<Expression> expressionColumns) { this.expressionColumns = expressionColumns; }
    public boolean isDeferrable() { return deferrable; }
    public void setDeferrable(boolean deferrable) { this.deferrable = deferrable; }
    public boolean isInitiallyDeferred() { return initiallyDeferred; }
    public void setInitiallyDeferred(boolean initiallyDeferred) { this.initiallyDeferred = initiallyDeferred; }
    public boolean isNotEnforced() { return notEnforced; }
    public void setNotEnforced(boolean notEnforced) { this.notEnforced = notEnforced; }
    public boolean isNoInherit() { return noInherit; }
    public void setNoInherit(boolean noInherit) { this.noInherit = noInherit; }
    public boolean isFromIndex() { return fromIndex; }
    public void setFromIndex(boolean fromIndex) { this.fromIndex = fromIndex; }
    public boolean isPromotedFromIndex() { return promotedFromIndex; }
    public void setPromotedFromIndex(boolean promotedFromIndex) { this.promotedFromIndex = promotedFromIndex; }
    public boolean isConvalidated() { return convalidated; }
    public void setConvalidated(boolean convalidated) { this.convalidated = convalidated; }
    public String getMatchType() { return matchType; }
    public void setMatchType(String matchType) { this.matchType = matchType; }
    public List<String> getOnDeleteSetNullColumns() { return onDeleteSetNullColumns; }
    public void setOnDeleteSetNullColumns(List<String> cols) { this.onDeleteSetNullColumns = cols; }
    public List<String> getOnUpdateSetNullColumns() { return onUpdateSetNullColumns; }
    public void setOnUpdateSetNullColumns(List<String> cols) { this.onUpdateSetNullColumns = cols; }

    /** Returns true if this constraint should be deferred (checked at commit time). */
    public boolean isCurrentlyDeferred() {
        return deferrable && initiallyDeferred;
    }

    /**
     * Rewrites every reference this constraint holds to a column of its OWN table after that
     * column is renamed: the key column list, EXCLUDE elements, ON DELETE/UPDATE SET NULL
     * column lists, and any expressions (CHECK, partial-index WHERE, expression index columns).
     * Without this, renaming a column silently detaches PK/UNIQUE enforcement (the constraint
     * still names the old column) and breaks CHECK evaluation with 42703 on every DML.
     */
    void renameColumn(String oldName, String newName) {
        columns = renameInList(columns, oldName, newName);
        onDeleteSetNullColumns = renameInList(onDeleteSetNullColumns, oldName, newName);
        onUpdateSetNullColumns = renameInList(onUpdateSetNullColumns, oldName, newName);
        if (excludeElements != null) {
            List<ExcludeElement> updated = new java.util.ArrayList<>();
            boolean changed = false;
            for (ExcludeElement e : excludeElements) {
                if (e.column != null && e.column.equalsIgnoreCase(oldName)) {
                    updated.add(new ExcludeElement(newName, e.operator));
                    changed = true;
                } else {
                    updated.add(e);
                }
            }
            if (changed) excludeElements = updated;
        }
        checkExpr = renameInExpr(checkExpr, oldName, newName);
        whereExpr = renameInExpr(whereExpr, oldName, newName);
        if (expressionColumns != null) {
            List<Expression> updated = new java.util.ArrayList<>();
            for (Expression e : expressionColumns) {
                updated.add(renameInExpr(e, oldName, newName));
            }
            expressionColumns = updated;
        }
    }

    /**
     * Rewrites references to a renamed column of the table this FOREIGN KEY points AT
     * (the referenced table's column, not this table's own columns).
     */
    void renameReferencedColumn(String oldName, String newName) {
        referencesColumns = renameInList(referencesColumns, oldName, newName);
    }

    /**
     * True if this constraint depends on the given column of its own table: as a key column,
     * an EXCLUDE element, or referenced from a CHECK / partial-index WHERE / expression-index
     * expression. Used by DROP COLUMN to drop dependent constraints, as PostgreSQL does.
     */
    boolean dependsOnColumn(String columnName) {
        if (containsIgnoreCase(columns, columnName)) return true;
        if (excludeElements != null) {
            for (ExcludeElement e : excludeElements) {
                if (e.column != null && e.column.equalsIgnoreCase(columnName)) return true;
            }
        }
        if (exprReferences(checkExpr, columnName)) return true;
        if (exprReferences(whereExpr, columnName)) return true;
        if (expressionColumns != null) {
            for (Expression e : expressionColumns) {
                if (exprReferences(e, columnName)) return true;
            }
        }
        return false;
    }

    static boolean containsIgnoreCase(List<String> list, String value) {
        if (list == null) return false;
        for (String s : list) {
            if (s != null && s.equalsIgnoreCase(value)) return true;
        }
        return false;
    }

    /** Case-insensitive whole-word rename of an identifier inside a SQL text fragment. */
    static String renameIdentifier(String text, String oldName, String newName) {
        return text.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(oldName) + "\\b",
                java.util.regex.Matcher.quoteReplacement(newName));
    }

    private static List<String> renameInList(List<String> cols, String oldName, String newName) {
        if (cols == null || cols.isEmpty()) return cols;
        boolean changed = false;
        List<String> out = new java.util.ArrayList<>(cols.size());
        for (String c : cols) {
            if (c != null && c.equalsIgnoreCase(oldName)) {
                out.add(newName);
                changed = true;
            } else {
                out.add(c);
            }
        }
        return changed ? Cols.listCopyOf(out) : cols;
    }

    /**
     * Renames a column reference inside an expression by unparsing to SQL, replacing the
     * identifier (whole-word, case-insensitive) and re-parsing — the same approach used for
     * dependent views. If unparse/re-parse fails, the original expression is kept unchanged.
     */
    static Expression renameInExpr(Expression expr, String oldName, String newName) {
        if (expr == null) return null;
        try {
            String sql = SqlUnparser.exprToSql(expr);
            String updated = renameIdentifier(sql, oldName, newName);
            if (updated.equals(sql)) return expr;
            return com.memgres.engine.parser.Parser.parseExpression(updated);
        } catch (Exception e) {
            return expr;
        }
    }

    /** True if the expression references the given column (whole-word, case-insensitive). */
    static boolean exprReferences(Expression expr, String columnName) {
        if (expr == null) return false;
        try {
            String sql = SqlUnparser.exprToSql(expr);
            return java.util.regex.Pattern
                    .compile("(?i)\\b" + java.util.regex.Pattern.quote(columnName) + "\\b")
                    .matcher(sql).find();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Creates an independent copy of this constraint for a partition that inherits it from
     * a parent (or ancestor) table. Row storage for a partitioned table lives entirely on the
     * leaf partitions, so each partition must enforce its own PK/UNIQUE constraints via its own
     * {@link TableIndex} rather than sharing the parent's {@code StoredConstraint} instance:
     * that instance is mutable (see {@code setConvalidated}, {@code setNotEnforced}, etc.), and
     * operations like {@code ALTER TABLE ... VALIDATE CONSTRAINT} mutate whichever instance they
     * find by name — sharing it would let a change made through one table silently leak to
     * every sibling that happens to reference the same object.
     * <p>
     * Mirrors PostgreSQL, which gives each partition's inherited constraint its own
     * auto-generated, partition-scoped name (e.g. {@code <partition>_pkey}) rather than reusing
     * the parent's constraint name — even though memgres additionally namespaces constraints
     * and their backing indexes per-{@link Table} instance, so a bare name collision across
     * tables would not by itself cause incorrect lookups.
     *
     * @param partitionTableName the name of the partition the copy will be attached to
     */
    public StoredConstraint copyForPartition(String partitionTableName) {
        String newName = name;
        if (type == Type.PRIMARY_KEY) {
            newName = partitionTableName + "_pkey";
        } else if (type == Type.UNIQUE) {
            newName = partitionTableName + "_" + String.join("_", columns) + "_key";
        }
        StoredConstraint copy = new StoredConstraint(newName, type, columns, checkExpr,
                referencesTable, referencesColumns, onDelete, onUpdate);
        copy.referencesSchema = referencesSchema;
        copy.excludeElements = excludeElements;
        copy.nullsNotDistinct = nullsNotDistinct;
        copy.deferrable = deferrable;
        copy.initiallyDeferred = initiallyDeferred;
        copy.notEnforced = notEnforced;
        copy.noInherit = noInherit;
        copy.convalidated = convalidated;
        copy.fromIndex = fromIndex;
        copy.promotedFromIndex = promotedFromIndex;
        copy.matchType = matchType;
        copy.whereExpr = whereExpr;
        copy.expressionColumns = expressionColumns;
        copy.onDeleteSetNullColumns = onDeleteSetNullColumns;
        copy.onUpdateSetNullColumns = onUpdateSetNullColumns;
        return copy;
    }

    public static FkAction parseFkAction(String action) {
        if (action == null) return FkAction.NO_ACTION;
        // Strip column list suffix (e.g., "SET NULL:a,b" -> "SET NULL")
        String base = action.contains(":") ? action.substring(0, action.indexOf(':')) : action;
        switch (base.toUpperCase().replace(" ", "_")) {
            case "CASCADE":
                return FkAction.CASCADE;
            case "SET_NULL":
                return FkAction.SET_NULL;
            case "SET_DEFAULT":
                return FkAction.SET_DEFAULT;
            case "RESTRICT":
                return FkAction.RESTRICT;
            default:
                return FkAction.NO_ACTION;
        }
    }

    /** Extract SET NULL column list from action string like "SET NULL:a,b". Returns null if no list. */
    public static List<String> parseSetNullColumns(String action) {
        if (action == null || !action.contains(":")) return null;
        String colPart = action.substring(action.indexOf(':') + 1);
        if (colPart.isEmpty()) return null;
        return java.util.Arrays.asList(colPart.split(","));
    }
}
