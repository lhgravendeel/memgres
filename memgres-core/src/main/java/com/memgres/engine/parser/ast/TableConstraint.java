package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * Table-level constraint in CREATE TABLE.
 */
public final class TableConstraint {
    public final String name;
    public final ConstraintType type;
    public final List<String> columns;
    public final Expression checkExpr;
    public final String referencesTable;
    public final List<String> referencesColumns;
    public final String onDelete;
    public final String onUpdate;
    public final boolean nullsNotDistinct;
    public final boolean deferrable;
    public final boolean initiallyDeferred;
    public final boolean notEnforced;
    public final List<ExcludeElement> excludeElements;

    public TableConstraint(
            String name,
            ConstraintType type,
            List<String> columns,
            Expression checkExpr,
            String referencesTable,
            List<String> referencesColumns,
            String onDelete,
            String onUpdate,
            boolean nullsNotDistinct,
            boolean deferrable,
            boolean initiallyDeferred,
            boolean notEnforced,
            List<ExcludeElement> excludeElements
    ) {
        this.name = name;
        this.type = type;
        this.columns = columns;
        this.checkExpr = checkExpr;
        this.referencesTable = referencesTable;
        this.referencesColumns = referencesColumns;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
        this.nullsNotDistinct = nullsNotDistinct;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
        this.notEnforced = notEnforced;
        this.excludeElements = excludeElements;
    }

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

    // Full constructor without excludeElements
    public TableConstraint(String name, ConstraintType type, List<String> columns,
                           Expression checkExpr, String referencesTable,
                           List<String> referencesColumns, String onDelete, String onUpdate,
                           boolean nullsNotDistinct, boolean deferrable, boolean initiallyDeferred) {
        this(name, type, columns, checkExpr, referencesTable, referencesColumns,
                onDelete, onUpdate, nullsNotDistinct, deferrable, initiallyDeferred, false, null);
    }

    // Constructor without deferrable fields
    public TableConstraint(String name, ConstraintType type, List<String> columns,
                           Expression checkExpr, String referencesTable,
                           List<String> referencesColumns, String onDelete, String onUpdate,
                           boolean nullsNotDistinct) {
        this(name, type, columns, checkExpr, referencesTable, referencesColumns,
                onDelete, onUpdate, nullsNotDistinct, false, false, false, null);
    }

    // Backwards-compatible constructor without nullsNotDistinct
    public TableConstraint(String name, ConstraintType type, List<String> columns,
                           Expression checkExpr, String referencesTable,
                           List<String> referencesColumns, String onDelete, String onUpdate) {
        this(name, type, columns, checkExpr, referencesTable, referencesColumns,
                onDelete, onUpdate, false, false, false, false, null);
    }

    public enum ConstraintType {
        PRIMARY_KEY, UNIQUE, CHECK, FOREIGN_KEY, EXCLUDE, NOT_NULL
    }

    public String name() { return name; }
    public ConstraintType type() { return type; }
    public List<String> columns() { return columns; }
    public Expression checkExpr() { return checkExpr; }
    public String referencesTable() { return referencesTable; }
    public List<String> referencesColumns() { return referencesColumns; }
    public String onDelete() { return onDelete; }
    public String onUpdate() { return onUpdate; }
    public boolean nullsNotDistinct() { return nullsNotDistinct; }
    public boolean deferrable() { return deferrable; }
    public boolean initiallyDeferred() { return initiallyDeferred; }
    public boolean notEnforced() { return notEnforced; }
    public List<ExcludeElement> excludeElements() { return excludeElements; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableConstraint that = (TableConstraint) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(type, that.type)
            && java.util.Objects.equals(columns, that.columns)
            && java.util.Objects.equals(checkExpr, that.checkExpr)
            && java.util.Objects.equals(referencesTable, that.referencesTable)
            && java.util.Objects.equals(referencesColumns, that.referencesColumns)
            && java.util.Objects.equals(onDelete, that.onDelete)
            && java.util.Objects.equals(onUpdate, that.onUpdate)
            && nullsNotDistinct == that.nullsNotDistinct
            && deferrable == that.deferrable
            && initiallyDeferred == that.initiallyDeferred
            && notEnforced == that.notEnforced
            && java.util.Objects.equals(excludeElements, that.excludeElements);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, type, columns, checkExpr, referencesTable, referencesColumns, onDelete, onUpdate, nullsNotDistinct, deferrable, initiallyDeferred, notEnforced, excludeElements);
    }

    @Override
    public String toString() {
        return "TableConstraint[name=" + name + ", " + "type=" + type + ", " + "columns=" + columns + ", " + "checkExpr=" + checkExpr + ", " + "referencesTable=" + referencesTable + ", " + "referencesColumns=" + referencesColumns + ", " + "onDelete=" + onDelete + ", " + "onUpdate=" + onUpdate + ", " + "nullsNotDistinct=" + nullsNotDistinct + ", " + "deferrable=" + deferrable + ", " + "initiallyDeferred=" + initiallyDeferred + ", " + "excludeElements=" + excludeElements + "]";
    }
}
