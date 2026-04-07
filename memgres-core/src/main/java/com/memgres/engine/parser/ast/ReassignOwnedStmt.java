package com.memgres.engine.parser.ast;

/** REASSIGN OWNED BY old_role TO new_role */
public final class ReassignOwnedStmt implements Statement {
    public final String oldRole;
    public final String newRole;

    public ReassignOwnedStmt(String oldRole, String newRole) {
        this.oldRole = oldRole;
        this.newRole = newRole;
    }

    public String oldRole() { return oldRole; }
    public String newRole() { return newRole; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReassignOwnedStmt that = (ReassignOwnedStmt) o;
        return java.util.Objects.equals(oldRole, that.oldRole)
            && java.util.Objects.equals(newRole, that.newRole);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(oldRole, newRole);
    }

    @Override
    public String toString() {
        return "ReassignOwnedStmt[oldRole=" + oldRole + ", " + "newRole=" + newRole + "]";
    }
}
