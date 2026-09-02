package com.memgres.engine.parser.ast;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * ALTER DEFAULT PRIVILEGES [FOR ROLE name] [IN SCHEMA name] grant_or_revoke
 */
public final class AlterDefaultPrivilegesStmt implements Statement {
    public final String forRole;
    public final String inSchema;
    public final boolean isGrant;            // false for REVOKE
    public final List<String> privileges;
    public final String objectType;          // "SCHEMAS"
    public final List<String> grantees;

    /** Every role the statement said FOR, and every schema it said IN: both clauses take a list. */
    public final List<String> forRoles;
    public final List<String> inSchemas;

    public AlterDefaultPrivilegesStmt(
            String forRole,
            String inSchema,
            boolean isGrant,
            List<String> privileges,
            String objectType,
            List<String> grantees
    ) {
        this(forRole, inSchema, isGrant, privileges, objectType, grantees,
                forRole == null ? Cols.<String>listOf() : Cols.listOf(forRole),
                inSchema == null ? Cols.<String>listOf() : Cols.listOf(inSchema));
    }

    public AlterDefaultPrivilegesStmt(
            String forRole,
            String inSchema,
            boolean isGrant,
            List<String> privileges,
            String objectType,
            List<String> grantees,
            List<String> forRoles,
            List<String> inSchemas
    ) {
        this.forRole = forRole;
        this.inSchema = inSchema;
        this.isGrant = isGrant;
        this.privileges = privileges;
        this.objectType = objectType;
        this.grantees = grantees;
        this.forRoles = forRoles;
        this.inSchemas = inSchemas;
    }

    public List<String> forRoles() { return forRoles; }
    public List<String> inSchemas() { return inSchemas; }

    /** Backward-compatible constructor without grantees. */
    public AlterDefaultPrivilegesStmt(String forRole, String inSchema, boolean isGrant,
                                      List<String> privileges, String objectType) {
        this(forRole, inSchema, isGrant, privileges, objectType, Cols.listOf());
    }

    public String forRole() { return forRole; }
    public String inSchema() { return inSchema; }
    public boolean isGrant() { return isGrant; }
    public List<String> privileges() { return privileges; }
    public String objectType() { return objectType; }
    public List<String> grantees() { return grantees; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterDefaultPrivilegesStmt that = (AlterDefaultPrivilegesStmt) o;
        return java.util.Objects.equals(forRole, that.forRole)
            && java.util.Objects.equals(inSchema, that.inSchema)
            && isGrant == that.isGrant
            && java.util.Objects.equals(privileges, that.privileges)
            && java.util.Objects.equals(objectType, that.objectType)
            && java.util.Objects.equals(grantees, that.grantees);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(forRole, inSchema, isGrant, privileges, objectType, grantees);
    }

    @Override
    public String toString() {
        return "AlterDefaultPrivilegesStmt[forRole=" + forRole + ", " + "inSchema=" + inSchema + ", " + "isGrant=" + isGrant + ", " + "privileges=" + privileges + ", " + "objectType=" + objectType + ", " + "grantees=" + grantees + "]";
    }
}
