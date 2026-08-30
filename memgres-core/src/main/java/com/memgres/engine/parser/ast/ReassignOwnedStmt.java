package com.memgres.engine.parser.ast;

/** REASSIGN OWNED BY old_role TO new_role */
public final class ReassignOwnedStmt implements Statement {
    public final String oldRole;
    public final String newRole;

    public ReassignOwnedStmt(String oldRole, String newRole) {
        this(java.util.Collections.singletonList(oldRole), newRole);
    }

    public ReassignOwnedStmt(java.util.List<String> oldRoles, String newRole) {
        this.oldRoles = oldRoles;
        this.oldRole = oldRoles.isEmpty() ? null : oldRoles.get(0);
        this.newRole = newRole;
    }

    /**
     * Every role the statement named. One statement may name several, and it reassigns what all
     * of them own; read as one name with the rest advanced over, the others kept their objects
     * and the statement reported success.
     */
    public java.util.List<String> oldRoles() { return oldRoles; }

    private final java.util.List<String> oldRoles;

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
