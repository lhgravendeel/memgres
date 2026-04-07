package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * REVOKE privileges ON object FROM role [CASCADE|RESTRICT]
 * REVOKE [GRANT OPTION FOR] privileges ON object FROM role
 * REVOKE [ADMIN OPTION FOR] role FROM role
 */
public final class RevokeStmt implements Statement {
    public final List<String> privileges;
    public final String objectType;
    public final String objectName;
    public final List<String> grantees;
    public final boolean grantOptionFor;
    public final boolean isRoleGrant;
    public final boolean cascade;

    public RevokeStmt(
            List<String> privileges,
            String objectType,
            String objectName,
            List<String> grantees,
            boolean grantOptionFor,
            boolean isRoleGrant,
            boolean cascade
    ) {
        this.privileges = privileges;
        this.objectType = objectType;
        this.objectName = objectName;
        this.grantees = grantees;
        this.grantOptionFor = grantOptionFor;
        this.isRoleGrant = isRoleGrant;
        this.cascade = cascade;
    }

    public List<String> privileges() { return privileges; }
    public String objectType() { return objectType; }
    public String objectName() { return objectName; }
    public List<String> grantees() { return grantees; }
    public boolean grantOptionFor() { return grantOptionFor; }
    public boolean isRoleGrant() { return isRoleGrant; }
    public boolean cascade() { return cascade; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RevokeStmt that = (RevokeStmt) o;
        return java.util.Objects.equals(privileges, that.privileges)
            && java.util.Objects.equals(objectType, that.objectType)
            && java.util.Objects.equals(objectName, that.objectName)
            && java.util.Objects.equals(grantees, that.grantees)
            && grantOptionFor == that.grantOptionFor
            && isRoleGrant == that.isRoleGrant
            && cascade == that.cascade;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(privileges, objectType, objectName, grantees, grantOptionFor, isRoleGrant, cascade);
    }

    @Override
    public String toString() {
        return "RevokeStmt[privileges=" + privileges + ", " + "objectType=" + objectType + ", " + "objectName=" + objectName + ", " + "grantees=" + grantees + ", " + "grantOptionFor=" + grantOptionFor + ", " + "isRoleGrant=" + isRoleGrant + ", " + "cascade=" + cascade + "]";
    }
}
