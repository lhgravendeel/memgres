package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE POLICY name ON table
 *   [AS {PERMISSIVE|RESTRICTIVE}]
 *   [FOR {ALL|SELECT|INSERT|UPDATE|DELETE}]
 *   [TO {role|PUBLIC|CURRENT_USER|SESSION_USER} [,...]]
 *   [USING (expr)]
 *   [WITH CHECK (expr)]
 */
public final class CreatePolicyStmt implements Statement {
    public final String name;
    public final String table;
    public final String command;             // ALL (default ALL)
    public final Expression usingExpr;
    public final Expression withCheckExpr;
    public final String policyType;          // PERMISSIVE (default) or RESTRICTIVE
    public final List<String> roles;
    /**
     * The schema written in front of the relation, or null when the name was written bare. A
     * policy belongs to its relation rather than to the relation's name, so two schemas may each
     * hold a relation of one name and each carry policies of its own.
     */
    public final String schema;

    public CreatePolicyStmt(
            String name,
            String table,
            String command,
            Expression usingExpr,
            Expression withCheckExpr,
            String policyType,
            List<String> roles,
            String schema
    ) {
        this.name = name;
        this.table = table;
        this.command = command;
        this.usingExpr = usingExpr;
        this.withCheckExpr = withCheckExpr;
        this.policyType = policyType;
        this.roles = roles;
        this.schema = schema;
    }

    /** Constructor for a relation whose name was written without a schema. */
    public CreatePolicyStmt(
            String name,
            String table,
            String command,
            Expression usingExpr,
            Expression withCheckExpr,
            String policyType,
            List<String> roles
    ) {
        this(name, table, command, usingExpr, withCheckExpr, policyType, roles, null);
    }

    /** Backward-compatible constructor without policyType and roles. */
    public CreatePolicyStmt(String name, String table, String command,
                            Expression usingExpr, Expression withCheckExpr) {
        this(name, table, command, usingExpr, withCheckExpr, "PERMISSIVE", null);
    }

    public String name() { return name; }
    public String table() { return table; }
    public String schema() { return schema; }
    public String command() { return command; }
    public Expression usingExpr() { return usingExpr; }
    public Expression withCheckExpr() { return withCheckExpr; }
    public String policyType() { return policyType; }
    public List<String> roles() { return roles; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreatePolicyStmt that = (CreatePolicyStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(command, that.command)
            && java.util.Objects.equals(usingExpr, that.usingExpr)
            && java.util.Objects.equals(withCheckExpr, that.withCheckExpr)
            && java.util.Objects.equals(policyType, that.policyType)
            && java.util.Objects.equals(roles, that.roles)
            && java.util.Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, table, command, usingExpr, withCheckExpr, policyType, roles, schema);
    }

    @Override
    public String toString() {
        return "CreatePolicyStmt[name=" + name + ", " + "table=" + table + ", " + "schema=" + schema + ", " + "command=" + command + ", " + "usingExpr=" + usingExpr + ", " + "withCheckExpr=" + withCheckExpr + ", " + "policyType=" + policyType + ", " + "roles=" + roles + "]";
    }
}
