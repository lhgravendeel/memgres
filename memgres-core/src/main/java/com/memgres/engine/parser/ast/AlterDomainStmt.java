package com.memgres.engine.parser.ast;

/**
 * ALTER DOMAIN name SET DEFAULT expr
 * ALTER DOMAIN name DROP DEFAULT
 * ALTER DOMAIN name ADD CONSTRAINT name CHECK (expr) [NOT VALID]
 * ALTER DOMAIN name DROP CONSTRAINT name
 * ALTER DOMAIN name VALIDATE CONSTRAINT name
 * ALTER DOMAIN name RENAME CONSTRAINT old TO new
 */
public final class AlterDomainStmt implements Statement {
    public final String domainName;
    public final String action;              // NO_OP
    public final String defaultValue;        // for SET DEFAULT (raw SQL text)
    public final String constraintName;      // for ADD/DROP/VALIDATE/RENAME CONSTRAINT
    public final Expression checkExpr;       // for ADD CONSTRAINT CHECK (expr)
    public final String rawCheckExpr;
    public final boolean notValid;           // for ADD CONSTRAINT ... NOT VALID
    public final String newConstraintName;   // for RENAME CONSTRAINT ... TO newName

    public AlterDomainStmt(
            String domainName,
            String action,
            String defaultValue,
            String constraintName,
            Expression checkExpr,
            String rawCheckExpr
    ) {
        this(domainName, action, defaultValue, constraintName, checkExpr, rawCheckExpr, false, null);
    }

    public AlterDomainStmt(
            String domainName,
            String action,
            String defaultValue,
            String constraintName,
            Expression checkExpr,
            String rawCheckExpr,
            boolean notValid,
            String newConstraintName
    ) {
        this.domainName = domainName;
        this.action = action;
        this.defaultValue = defaultValue;
        this.constraintName = constraintName;
        this.checkExpr = checkExpr;
        this.rawCheckExpr = rawCheckExpr;
        this.notValid = notValid;
        this.newConstraintName = newConstraintName;
    }

    public String domainName() { return domainName; }
    public String action() { return action; }
    public String defaultValue() { return defaultValue; }
    public String constraintName() { return constraintName; }
    public Expression checkExpr() { return checkExpr; }
    public String rawCheckExpr() { return rawCheckExpr; }
    public boolean notValid() { return notValid; }
    public String newConstraintName() { return newConstraintName; }

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
            && java.util.Objects.equals(rawCheckExpr, that.rawCheckExpr)
            && notValid == that.notValid
            && java.util.Objects.equals(newConstraintName, that.newConstraintName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(domainName, action, defaultValue, constraintName, checkExpr, rawCheckExpr, notValid, newConstraintName);
    }

    @Override
    public String toString() {
        return "AlterDomainStmt[domainName=" + domainName + ", " + "action=" + action + ", " + "defaultValue=" + defaultValue + ", " + "constraintName=" + constraintName + ", " + "checkExpr=" + checkExpr + ", " + "rawCheckExpr=" + rawCheckExpr + ", notValid=" + notValid + ", newConstraintName=" + newConstraintName + "]";
    }
}
