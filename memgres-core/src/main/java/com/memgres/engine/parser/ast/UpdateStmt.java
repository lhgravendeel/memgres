package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * UPDATE [schema.]table [AS alias] SET col=val, ... [FROM ...] [WHERE ...] [RETURNING ...]
 */
public final class UpdateStmt implements Statement {
    public final String schema;
    public final String table;
    public final String alias;
    public final List<InsertStmt.SetClause> setClauses;
    public final List<SelectStmt.FromItem> from;
    public final Expression where;
    public final List<SelectStmt.SelectTarget> returning;
    public final List<SelectStmt.CommonTableExpr> withClauses;

    public UpdateStmt(
            String schema,
            String table,
            String alias,
            List<InsertStmt.SetClause> setClauses,
            List<SelectStmt.FromItem> from,
            Expression where,
            List<SelectStmt.SelectTarget> returning,
            List<SelectStmt.CommonTableExpr> withClauses
    ) {
        this.schema = schema;
        this.table = table;
        this.alias = alias;
        this.setClauses = setClauses;
        this.from = from;
        this.where = where;
        this.returning = returning;
        this.withClauses = withClauses;
    }

    /** Constructor without alias. */
    public UpdateStmt(String schema, String table, List<InsertStmt.SetClause> setClauses,
                      List<SelectStmt.FromItem> from, Expression where,
                      List<SelectStmt.SelectTarget> returning,
                      List<SelectStmt.CommonTableExpr> withClauses) {
        this(schema, table, null, setClauses, from, where, returning, withClauses);
    }

    /** Backward-compatible constructor without WITH clauses. */
    public UpdateStmt(String schema, String table, List<InsertStmt.SetClause> setClauses,
                      List<SelectStmt.FromItem> from, Expression where,
                      List<SelectStmt.SelectTarget> returning) {
        this(schema, table, null, setClauses, from, where, returning, null);
    }

    public String schema() { return schema; }
    public String table() { return table; }
    public String alias() { return alias; }
    public List<InsertStmt.SetClause> setClauses() { return setClauses; }
    public List<SelectStmt.FromItem> from() { return from; }
    public Expression where() { return where; }
    public List<SelectStmt.SelectTarget> returning() { return returning; }
    public List<SelectStmt.CommonTableExpr> withClauses() { return withClauses; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateStmt that = (UpdateStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(alias, that.alias)
            && java.util.Objects.equals(setClauses, that.setClauses)
            && java.util.Objects.equals(from, that.from)
            && java.util.Objects.equals(where, that.where)
            && java.util.Objects.equals(returning, that.returning)
            && java.util.Objects.equals(withClauses, that.withClauses);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, table, alias, setClauses, from, where, returning, withClauses);
    }

    @Override
    public String toString() {
        return "UpdateStmt[schema=" + schema + ", " + "table=" + table + ", " + "alias=" + alias + ", " + "setClauses=" + setClauses + ", " + "from=" + from + ", " + "where=" + where + ", " + "returning=" + returning + ", " + "withClauses=" + withClauses + "]";
    }
}
