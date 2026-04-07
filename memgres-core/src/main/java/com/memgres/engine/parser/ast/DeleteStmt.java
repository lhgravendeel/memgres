package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * DELETE FROM [schema.]table [AS alias] [USING other_table ...] [WHERE ...] [RETURNING ...]
 */
public final class DeleteStmt implements Statement {
    public final String schema;
    public final String table;
    public final String alias;
    public final List<SelectStmt.FromItem> using;
    public final Expression where;
    public final List<SelectStmt.SelectTarget> returning;
    public final List<SelectStmt.CommonTableExpr> withClauses;

    public DeleteStmt(
            String schema,
            String table,
            String alias,
            List<SelectStmt.FromItem> using,
            Expression where,
            List<SelectStmt.SelectTarget> returning,
            List<SelectStmt.CommonTableExpr> withClauses
    ) {
        this.schema = schema;
        this.table = table;
        this.alias = alias;
        this.using = using;
        this.where = where;
        this.returning = returning;
        this.withClauses = withClauses;
    }

    /** Constructor without alias. */
    public DeleteStmt(String schema, String table, List<SelectStmt.FromItem> using,
                      Expression where, List<SelectStmt.SelectTarget> returning,
                      List<SelectStmt.CommonTableExpr> withClauses) {
        this(schema, table, null, using, where, returning, withClauses);
    }

    /** Backward-compatible constructor without USING clause. */
    public DeleteStmt(String schema, String table, Expression where,
                      List<SelectStmt.SelectTarget> returning) {
        this(schema, table, null, null, where, returning, null);
    }

    /** Backward-compatible constructor without WITH clauses. */
    public DeleteStmt(String schema, String table, List<SelectStmt.FromItem> using,
                      Expression where, List<SelectStmt.SelectTarget> returning) {
        this(schema, table, null, using, where, returning, null);
    }

    public String schema() { return schema; }
    public String table() { return table; }
    public String alias() { return alias; }
    public List<SelectStmt.FromItem> using() { return using; }
    public Expression where() { return where; }
    public List<SelectStmt.SelectTarget> returning() { return returning; }
    public List<SelectStmt.CommonTableExpr> withClauses() { return withClauses; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteStmt that = (DeleteStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(alias, that.alias)
            && java.util.Objects.equals(using, that.using)
            && java.util.Objects.equals(where, that.where)
            && java.util.Objects.equals(returning, that.returning)
            && java.util.Objects.equals(withClauses, that.withClauses);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, table, alias, using, where, returning, withClauses);
    }

    @Override
    public String toString() {
        return "DeleteStmt[schema=" + schema + ", " + "table=" + table + ", " + "alias=" + alias + ", " + "using=" + using + ", " + "where=" + where + ", " + "returning=" + returning + ", " + "withClauses=" + withClauses + "]";
    }
}
