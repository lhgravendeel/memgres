package com.memgres.engine.parser.ast;

/**
 * ALTER POLICY name ON table [TO roles] [USING (expr)] [WITH CHECK (expr)]
 * ALTER POLICY name ON table RENAME TO newname
 */
public final class AlterPolicyStmt implements Statement {
    public final String name;
    public final String table;
    public final String renameTo;
    public final Expression usingExpr;
    public final Expression withCheckExpr;

    public AlterPolicyStmt(
            String name,
            String table,
            String renameTo,
            Expression usingExpr,
            Expression withCheckExpr
    ) {
        this.name = name;
        this.table = table;
        this.renameTo = renameTo;
        this.usingExpr = usingExpr;
        this.withCheckExpr = withCheckExpr;
    }

    public String name() { return name; }
    public String table() { return table; }
    public String renameTo() { return renameTo; }
    public Expression usingExpr() { return usingExpr; }
    public Expression withCheckExpr() { return withCheckExpr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterPolicyStmt that = (AlterPolicyStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(renameTo, that.renameTo)
            && java.util.Objects.equals(usingExpr, that.usingExpr)
            && java.util.Objects.equals(withCheckExpr, that.withCheckExpr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, table, renameTo, usingExpr, withCheckExpr);
    }

    @Override
    public String toString() {
        return "AlterPolicyStmt[name=" + name + ", " + "table=" + table + ", " + "renameTo=" + renameTo + ", " + "usingExpr=" + usingExpr + ", " + "withCheckExpr=" + withCheckExpr + "]";
    }
}
