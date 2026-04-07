package com.memgres.engine.parser.ast;

/**
 * ALTER DOMAIN name SET DEFAULT expr
 * ALTER DOMAIN name DROP DEFAULT
 * ALTER DOMAIN name ADD CONSTRAINT name CHECK (expr)
 * ALTER DOMAIN name DROP CONSTRAINT name
 * ALTER DOMAIN name VALIDATE CONSTRAINT name
 */
public final class AlterDomainStmt implements Statement {
    public final String domainName;
    public final String action;              // NO_OP
    public final String defaultValue;        // for SET DEFAULT (raw SQL text)
    public final String constraintName;      // for ADD/DROP/VALIDATE CONSTRAINT
    public final Expression checkExpr;       // for ADD CONSTRAINT CHECK (expr)
    public final String rawCheckExpr;

    public AlterDomainStmt(
            String domainName,
            String action,
            String defaultValue,
            String constraintName,
            Expression checkExpr,
            String rawCheckExpr
    ) {
        this.domainName = domainName;
        this.action = action;
        this.defaultValue = defaultValue;
        this.constraintName = constraintName;
        this.checkExpr = checkExpr;
        this.rawCheckExpr = rawCheckExpr;
    }

    public String domainName() { return domainName; }
    public String action() { return action; }
    public String defaultValue() { return defaultValue; }
    public String constraintName() { return constraintName; }
    public Expression checkExpr() { return checkExpr; }
    public String rawCheckExpr() { return rawCheckExpr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterDomainStmt that = (AlterDomainStmt) o;
        return java.util.Objects.equals(domainName, that.domainName)
            && java.util.Objects.equals(action, that.action)
            && java.util.Objects.equals(defaultValue, that.defaultValue)
            && java.util.Objects.equals(constraintName, that.constraintName)
            && java.util.Objects.equals(checkExpr, that.checkExpr)
            && java.util.Objects.equals(rawCheckExpr, that.rawCheckExpr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(domainName, action, defaultValue, constraintName, checkExpr, rawCheckExpr);
    }

    @Override
    public String toString() {
        return "AlterDomainStmt[domainName=" + domainName + ", " + "action=" + action + ", " + "defaultValue=" + defaultValue + ", " + "constraintName=" + constraintName + ", " + "checkExpr=" + checkExpr + ", " + "rawCheckExpr=" + rawCheckExpr + "]";
    }
}
