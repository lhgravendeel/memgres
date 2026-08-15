package com.memgres.engine.parser.ast;

/**
 * CREATE DOMAIN name AS base_type [DEFAULT expr] [NOT NULL] [CONSTRAINT name] [CHECK (expr)]
 */
public final class CreateDomainStmt implements Statement {
    public final String name;
    public final String baseType;
    public final Expression defaultExpr;
    public final boolean notNull;
    public final Expression checkExpr;
    public final String constraintName; // explicit constraint name for CHECK, or null
    /**
     * Every CHECK the definition wrote, in the order it wrote them. A domain may carry any number
     * of them, each with a name of its own or with none, so the pair above is the head of this
     * list rather than the whole of what was written.
     */
    private java.util.List<DomainCheck> checks;
    private String collation;           // COLLATE clause, or null

    /** One CHECK of a domain: the name written before it, or null, and the condition itself. */
    public static final class DomainCheck {
        private final String name;
        private final Expression expr;

        public DomainCheck(String name, Expression expr) {
            this.name = name;
            this.expr = expr;
        }

        public String name() { return name; }
        public Expression expr() { return expr; }

        @Override
        public String toString() { return "DomainCheck[name=" + name + ", expr=" + expr + "]"; }
    }

    public java.util.List<DomainCheck> checks() { return checks; }
    public void setChecks(java.util.List<DomainCheck> checks) { this.checks = checks; }
    /** The schema the name was written in, or null when it was written bare. */
    private String schemaName;

    public String collation() { return collation; }
    public void setCollation(String collation) { this.collation = collation; }
    public String schemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public CreateDomainStmt(
            String name,
            String baseType,
            Expression defaultExpr,
            boolean notNull,
            Expression checkExpr
    ) {
        this(name, baseType, defaultExpr, notNull, checkExpr, null);
    }

    public CreateDomainStmt(
            String name,
            String baseType,
            Expression defaultExpr,
            boolean notNull,
            Expression checkExpr,
            String constraintName
    ) {
        this.name = name;
        this.baseType = baseType;
        this.defaultExpr = defaultExpr;
        this.notNull = notNull;
        this.checkExpr = checkExpr;
        this.constraintName = constraintName;
    }

    public String name() { return name; }
    public String baseType() { return baseType; }
    public Expression defaultExpr() { return defaultExpr; }
    public boolean notNull() { return notNull; }
    public Expression checkExpr() { return checkExpr; }
    public String constraintName() { return constraintName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateDomainStmt that = (CreateDomainStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(baseType, that.baseType)
            && java.util.Objects.equals(defaultExpr, that.defaultExpr)
            && notNull == that.notNull
            && java.util.Objects.equals(checkExpr, that.checkExpr)
            && java.util.Objects.equals(constraintName, that.constraintName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, baseType, defaultExpr, notNull, checkExpr, constraintName);
    }

    @Override
    public String toString() {
        return "CreateDomainStmt[name=" + name + ", " + "baseType=" + baseType + ", " + "defaultExpr=" + defaultExpr + ", " + "notNull=" + notNull + ", " + "checkExpr=" + checkExpr + ", " + "constraintName=" + constraintName + "]";
    }
}
