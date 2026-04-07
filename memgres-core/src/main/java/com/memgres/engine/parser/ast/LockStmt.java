package com.memgres.engine.parser.ast;

/**
 * LOCK [TABLE] [ONLY] name [, ...] [IN lockmode MODE] [NOWAIT]
 */
public final class LockStmt implements Statement {
    public final String tableName;
    public final String lockMode;
    public final boolean nowait;

    public LockStmt(String tableName, String lockMode, boolean nowait) {
        this.tableName = tableName;
        this.lockMode = lockMode;
        this.nowait = nowait;
    }

    public String tableName() { return tableName; }
    public String lockMode() { return lockMode; }
    public boolean nowait() { return nowait; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockStmt that = (LockStmt) o;
        return java.util.Objects.equals(tableName, that.tableName)
            && java.util.Objects.equals(lockMode, that.lockMode)
            && nowait == that.nowait;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(tableName, lockMode, nowait);
    }

    @Override
    public String toString() {
        return "LockStmt[tableName=" + tableName + ", " + "lockMode=" + lockMode + ", " + "nowait=" + nowait + "]";
    }
}
