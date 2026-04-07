package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * INSERT INTO [schema.]table (columns) VALUES (...), (...) [ON CONFLICT ...] [RETURNING ...]
 */
public final class InsertStmt implements Statement {
    public final String schema;
    public final String table;
    public final List<String> columns;
    public final List<List<Expression>> values;
    public final Statement selectStmt;
    public final OnConflict onConflict;
    public final List<SelectStmt.SelectTarget> returning;
    public final List<SelectStmt.CommonTableExpr> withClauses;
    public final String alias;
    public final boolean overridingSystemValue;
    public final boolean overridingUserValue;

    public InsertStmt(
            String schema,
            String table,
            List<String> columns,
            List<List<Expression>> values,
            Statement selectStmt,
            OnConflict onConflict,
            List<SelectStmt.SelectTarget> returning,
            List<SelectStmt.CommonTableExpr> withClauses,
            String alias,
            boolean overridingSystemValue,
            boolean overridingUserValue
    ) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.values = values;
        this.selectStmt = selectStmt;
        this.onConflict = onConflict;
        this.returning = returning;
        this.withClauses = withClauses;
        this.alias = alias;
        this.overridingSystemValue = overridingSystemValue;
        this.overridingUserValue = overridingUserValue;
    }

    /** Backward-compatible constructor without WITH clauses or alias. */
    public InsertStmt(String schema, String table, List<String> columns,
                      List<List<Expression>> values, SelectStmt selectStmt,
                      OnConflict onConflict, List<SelectStmt.SelectTarget> returning) {
        this(schema, table, columns, values, selectStmt, onConflict, returning, null, null, false, false);
    }

    /** Backward-compatible constructor without alias. */
    public InsertStmt(String schema, String table, List<String> columns,
                      List<List<Expression>> values, Statement selectStmt,
                      OnConflict onConflict, List<SelectStmt.SelectTarget> returning,
                      List<SelectStmt.CommonTableExpr> withClauses) {
        this(schema, table, columns, values, selectStmt, onConflict, returning, withClauses, null, false, false);
    }

    public InsertStmt(String schema, String table, List<String> columns,
                      List<List<Expression>> values, Statement selectStmt,
                      OnConflict onConflict, List<SelectStmt.SelectTarget> returning,
                      List<SelectStmt.CommonTableExpr> withClauses, String alias) {
        this(schema, table, columns, values, selectStmt, onConflict, returning, withClauses, alias, false, false);
    }

    /** Constructor with overridingSystemValue but not overridingUserValue. */
    public InsertStmt(String schema, String table, List<String> columns,
                      List<List<Expression>> values, Statement selectStmt,
                      OnConflict onConflict, List<SelectStmt.SelectTarget> returning,
                      List<SelectStmt.CommonTableExpr> withClauses, String alias,
                      boolean overridingSystemValue) {
        this(schema, table, columns, values, selectStmt, onConflict, returning, withClauses, alias, overridingSystemValue, false);
    }

    public static final class OnConflict {
        public final List<String> columns;
        public final String constraint;
        public final boolean doNothing;
        public final List<SetClause> doUpdate;
        public final Expression whereClause;
        public final List<String> conflictExpressions;

        public OnConflict(
                List<String> columns,
                String constraint,
                boolean doNothing,
                List<SetClause> doUpdate,
                Expression whereClause,
                List<String> conflictExpressions
        ) {
            this.columns = columns;
            this.constraint = constraint;
            this.doNothing = doNothing;
            this.doUpdate = doUpdate;
            this.whereClause = whereClause;
            this.conflictExpressions = conflictExpressions;
        }

        /** Convenience constructor without WHERE clause. */
        public OnConflict(List<String> columns, String constraint, boolean doNothing, List<SetClause> doUpdate) {
            this(columns, constraint, doNothing, doUpdate, null, null);
        }
        /** Convenience constructor without expression targets. */
        public OnConflict(List<String> columns, String constraint, boolean doNothing, List<SetClause> doUpdate, Expression whereClause) {
            this(columns, constraint, doNothing, doUpdate, whereClause, null);
        }

        public List<String> columns() { return columns; }
        public String constraint() { return constraint; }
        public boolean doNothing() { return doNothing; }
        public List<SetClause> doUpdate() { return doUpdate; }
        public Expression whereClause() { return whereClause; }
        public List<String> conflictExpressions() { return conflictExpressions; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OnConflict that = (OnConflict) o;
            return java.util.Objects.equals(columns, that.columns)
                && java.util.Objects.equals(constraint, that.constraint)
                && java.util.Objects.equals(doNothing, that.doNothing)
                && java.util.Objects.equals(doUpdate, that.doUpdate)
                && java.util.Objects.equals(whereClause, that.whereClause)
                && java.util.Objects.equals(conflictExpressions, that.conflictExpressions);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(columns, constraint, doNothing, doUpdate, whereClause, conflictExpressions);
        }

        @Override
        public String toString() {
            return "OnConflict[columns=" + columns + ", " + "constraint=" + constraint + ", " + "doNothing=" + doNothing + ", " + "doUpdate=" + doUpdate + ", " + "whereClause=" + whereClause + ", " + "conflictExpressions=" + conflictExpressions + "]";
        }
    }

    public static final class SetClause {
        public final String column;
        public final Expression value;

        public SetClause(String column, Expression value) {
            this.column = column;
            this.value = value;
        }

        public String column() { return column; }
        public Expression value() { return value; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetClause that = (SetClause) o;
            return java.util.Objects.equals(column, that.column)
                && java.util.Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(column, value);
        }

        @Override
        public String toString() {
            return "SetClause[column=" + column + ", " + "value=" + value + "]";
        }
    }

    public String schema() { return schema; }
    public String table() { return table; }
    public List<String> columns() { return columns; }
    public List<List<Expression>> values() { return values; }
    public Statement selectStmt() { return selectStmt; }
    public OnConflict onConflict() { return onConflict; }
    public List<SelectStmt.SelectTarget> returning() { return returning; }
    public List<SelectStmt.CommonTableExpr> withClauses() { return withClauses; }
    public String alias() { return alias; }
    public boolean overridingSystemValue() { return overridingSystemValue; }
    public boolean overridingUserValue() { return overridingUserValue; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InsertStmt that = (InsertStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(columns, that.columns)
            && java.util.Objects.equals(values, that.values)
            && java.util.Objects.equals(selectStmt, that.selectStmt)
            && java.util.Objects.equals(onConflict, that.onConflict)
            && java.util.Objects.equals(returning, that.returning)
            && java.util.Objects.equals(withClauses, that.withClauses)
            && java.util.Objects.equals(alias, that.alias)
            && java.util.Objects.equals(overridingSystemValue, that.overridingSystemValue)
            && java.util.Objects.equals(overridingUserValue, that.overridingUserValue);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, table, columns, values, selectStmt, onConflict, returning, withClauses, alias, overridingSystemValue, overridingUserValue);
    }

    @Override
    public String toString() {
        return "InsertStmt[schema=" + schema + ", " + "table=" + table + ", " + "columns=" + columns + ", " + "values=" + values + ", " + "selectStmt=" + selectStmt + ", " + "onConflict=" + onConflict + ", " + "returning=" + returning + ", " + "withClauses=" + withClauses + ", " + "alias=" + alias + ", " + "overridingSystemValue=" + overridingSystemValue + ", " + "overridingUserValue=" + overridingUserValue + "]";
    }
}
