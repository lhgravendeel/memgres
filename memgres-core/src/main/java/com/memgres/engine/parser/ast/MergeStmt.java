package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * MERGE INTO target_table [AS alias]
 * USING source ON join_condition
 * WHEN MATCHED [AND condition] THEN UPDATE SET ... / DELETE
 * WHEN NOT MATCHED [AND condition] THEN INSERT (cols) VALUES (exprs) / DO NOTHING
 */
public final class MergeStmt implements Statement {
    public final String schema;
    public final String targetTable;
    public final String targetAlias;
    public final SelectStmt.FromItem source;
    public final Expression onCondition;
    public final List<WhenClause> whenClauses;
    public final List<SelectStmt.SelectTarget> returning;

    public MergeStmt(
            String schema,
            String targetTable,
            String targetAlias,
            SelectStmt.FromItem source,
            Expression onCondition,
            List<WhenClause> whenClauses,
            List<SelectStmt.SelectTarget> returning
    ) {
        this.schema = schema;
        this.targetTable = targetTable;
        this.targetAlias = targetAlias;
        this.source = source;
        this.onCondition = onCondition;
        this.whenClauses = whenClauses;
        this.returning = returning;
    }

    public interface WhenClause {}

    public static final class WhenMatched implements WhenClause {
        public final Expression andCondition;
        public final boolean isDelete;
        public final List<InsertStmt.SetClause> setClauses;

        public WhenMatched(Expression andCondition, boolean isDelete, List<InsertStmt.SetClause> setClauses) {
            this.andCondition = andCondition;
            this.isDelete = isDelete;
            this.setClauses = setClauses;
        }

        public Expression andCondition() { return andCondition; }
        public boolean isDelete() { return isDelete; }
        public List<InsertStmt.SetClause> setClauses() { return setClauses; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhenMatched that = (WhenMatched) o;
            return Objects.equals(andCondition, that.andCondition)
                && isDelete == that.isDelete
                && Objects.equals(setClauses, that.setClauses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(andCondition, isDelete, setClauses);
        }

        @Override
        public String toString() {
            return "WhenMatched[andCondition=" + andCondition + ", isDelete=" + isDelete + ", setClauses=" + setClauses + "]";
        }
    }

    public static final class WhenNotMatched implements WhenClause {
        public final Expression andCondition;
        public final boolean doNothing;
        public final List<String> columns;
        public final List<Expression> values;

        public WhenNotMatched(Expression andCondition, boolean doNothing, List<String> columns, List<Expression> values) {
            this.andCondition = andCondition;
            this.doNothing = doNothing;
            this.columns = columns;
            this.values = values;
        }

        public Expression andCondition() { return andCondition; }
        public boolean doNothing() { return doNothing; }
        public List<String> columns() { return columns; }
        public List<Expression> values() { return values; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhenNotMatched that = (WhenNotMatched) o;
            return Objects.equals(andCondition, that.andCondition)
                && doNothing == that.doNothing
                && Objects.equals(columns, that.columns)
                && Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(andCondition, doNothing, columns, values);
        }

        @Override
        public String toString() {
            return "WhenNotMatched[andCondition=" + andCondition + ", doNothing=" + doNothing + ", columns=" + columns + ", values=" + values + "]";
        }
    }

    /**
     * WHEN NOT MATCHED BY SOURCE [AND condition] THEN UPDATE SET ... / DELETE / DO NOTHING
     * (PG 17+) — fires for target rows with no matching source row.
     */
    public static final class WhenNotMatchedBySource implements WhenClause {
        public final Expression andCondition;
        public final boolean isDelete;
        public final List<InsertStmt.SetClause> setClauses;

        public WhenNotMatchedBySource(Expression andCondition, boolean isDelete, List<InsertStmt.SetClause> setClauses) {
            this.andCondition = andCondition;
            this.isDelete = isDelete;
            this.setClauses = setClauses;
        }

        public Expression andCondition() { return andCondition; }
        public boolean isDelete() { return isDelete; }
        public List<InsertStmt.SetClause> setClauses() { return setClauses; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhenNotMatchedBySource that = (WhenNotMatchedBySource) o;
            return isDelete == that.isDelete
                && Objects.equals(andCondition, that.andCondition)
                && Objects.equals(setClauses, that.setClauses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(andCondition, isDelete, setClauses);
        }

        @Override
        public String toString() {
            return "WhenNotMatchedBySource[andCondition=" + andCondition + ", isDelete=" + isDelete + ", setClauses=" + setClauses + "]";
        }
    }

    public String schema() { return schema; }
    public String targetTable() { return targetTable; }
    public String targetAlias() { return targetAlias; }
    public SelectStmt.FromItem source() { return source; }
    public Expression onCondition() { return onCondition; }
    public List<WhenClause> whenClauses() { return whenClauses; }
    public List<SelectStmt.SelectTarget> returning() { return returning; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeStmt that = (MergeStmt) o;
        return Objects.equals(schema, that.schema)
            && Objects.equals(targetTable, that.targetTable)
            && Objects.equals(targetAlias, that.targetAlias)
            && Objects.equals(source, that.source)
            && Objects.equals(onCondition, that.onCondition)
            && Objects.equals(whenClauses, that.whenClauses)
            && Objects.equals(returning, that.returning);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, targetTable, targetAlias, source, onCondition, whenClauses, returning);
    }

    @Override
    public String toString() {
        return "MergeStmt[schema=" + schema + ", targetTable=" + targetTable + ", targetAlias=" + targetAlias
            + ", source=" + source + ", onCondition=" + onCondition + ", whenClauses=" + whenClauses
            + ", returning=" + returning + "]";
    }
}
