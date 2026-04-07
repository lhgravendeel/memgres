package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * GRANT privileges ON object TO role [WITH GRANT OPTION]
 * GRANT role TO role [WITH ADMIN OPTION]
 */
public final class GrantStmt implements Statement {
    public final List<String> privileges;
    public final String objectType;
    public final String objectName;
    public final List<String> grantees;
    public final boolean withGrantOption;
    public final boolean withAdminOption;
    public final boolean isRoleGrant;
    public final List<String> columns;

    public GrantStmt(
            List<String> privileges,
            String objectType,
            String objectName,
            List<String> grantees,
            boolean withGrantOption,
            boolean withAdminOption,
            boolean isRoleGrant,
            List<String> columns
    ) {
        this.privileges = privileges;
        this.objectType = objectType;
        this.objectName = objectName;
        this.grantees = grantees;
        this.withGrantOption = withGrantOption;
        this.withAdminOption = withAdminOption;
        this.isRoleGrant = isRoleGrant;
        this.columns = columns;
    }

    public List<String> privileges() { return privileges; }
    public String objectType() { return objectType; }
    public String objectName() { return objectName; }
    public List<String> grantees() { return grantees; }
    public boolean withGrantOption() { return withGrantOption; }
    public boolean withAdminOption() { return withAdminOption; }
    public boolean isRoleGrant() { return isRoleGrant; }
    public List<String> columns() { return columns; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GrantStmt that = (GrantStmt) o;
        return Objects.equals(privileges, that.privileges)
            && Objects.equals(objectType, that.objectType)
            && Objects.equals(objectName, that.objectName)
            && Objects.equals(grantees, that.grantees)
            && withGrantOption == that.withGrantOption
            && withAdminOption == that.withAdminOption
            && isRoleGrant == that.isRoleGrant
            && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privileges, objectType, objectName, grantees, withGrantOption, withAdminOption, isRoleGrant, columns);
    }

    @Override
    public String toString() {
        return "GrantStmt[privileges=" + privileges + ", objectType=" + objectType + ", objectName=" + objectName
            + ", grantees=" + grantees + ", withGrantOption=" + withGrantOption + ", withAdminOption=" + withAdminOption
            + ", isRoleGrant=" + isRoleGrant + ", columns=" + columns + "]";
    }
}
