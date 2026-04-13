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
    private final List<String> columns;
    private final Expression checkExpr;
    private final String referencesTable;
    private final List<String> referencesColumns;
    private final FkAction onDelete;
    private final FkAction onUpdate;
    private boolean nullsNotDistinct;
    private boolean deferrable;
    private boolean initiallyDeferred;
    private boolean notEnforced; // PG 18: NOT ENFORCED constraints are stored but not validated
    private boolean convalidated = true; // pg_constraint.convalidated: false when added with NOT VALID
    private boolean fromIndex; // true if this constraint was created via CREATE UNIQUE INDEX (not ADD CONSTRAINT)
    private boolean promotedFromIndex; // true if created via ADD CONSTRAINT ... UNIQUE USING INDEX
    private Expression whereExpr; // partial index predicate
    private List<Expression> expressionColumns; // parsed expressions for expression-based index columns

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
    public boolean isFromIndex() { return fromIndex; }
    public void setFromIndex(boolean fromIndex) { this.fromIndex = fromIndex; }
    public boolean isPromotedFromIndex() { return promotedFromIndex; }
    public void setPromotedFromIndex(boolean promotedFromIndex) { this.promotedFromIndex = promotedFromIndex; }
    public boolean isConvalidated() { return convalidated; }
    public void setConvalidated(boolean convalidated) { this.convalidated = convalidated; }

    /** Returns true if this constraint should be deferred (checked at commit time). */
    public boolean isCurrentlyDeferred() {
        return deferrable && initiallyDeferred;
    }

    public static FkAction parseFkAction(String action) {
        if (action == null) return FkAction.NO_ACTION;
        switch (action.toUpperCase().replace(" ", "_")) {
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
}
