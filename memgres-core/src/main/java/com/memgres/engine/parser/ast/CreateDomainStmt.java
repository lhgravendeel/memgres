package com.memgres.engine.parser.ast;

/**
 * CREATE DOMAIN name AS base_type [DEFAULT expr] [NOT NULL] [CHECK (expr)]
 */
public final class CreateDomainStmt implements Statement {
    public final String name;
    public final String baseType;
    public final Expression defaultExpr;
    public final boolean notNull;
    public final Expression checkExpr;

    public CreateDomainStmt(
            String name,
            String baseType,
            Expression defaultExpr,
            boolean notNull,
            Expression checkExpr
    ) {
        this.name = name;
        this.baseType = baseType;
        this.defaultExpr = defaultExpr;
        this.notNull = notNull;
        this.checkExpr = checkExpr;
    }

    public String name() { return name; }
    public String baseType() { return baseType; }
    public Expression defaultExpr() { return defaultExpr; }
    public boolean notNull() { return notNull; }
    public Expression checkExpr() { return checkExpr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateDomainStmt that = (CreateDomainStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(baseType, that.baseType)
            && java.util.Objects.equals(defaultExpr, that.defaultExpr)
            && notNull == that.notNull
            && java.util.Objects.equals(checkExpr, that.checkExpr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, baseType, defaultExpr, notNull, checkExpr);
    }

    @Override
    public String toString() {
        return "CreateDomainStmt[name=" + name + ", " + "baseType=" + baseType + ", " + "defaultExpr=" + defaultExpr + ", " + "notNull=" + notNull + ", " + "checkExpr=" + checkExpr + "]";
    }
}
