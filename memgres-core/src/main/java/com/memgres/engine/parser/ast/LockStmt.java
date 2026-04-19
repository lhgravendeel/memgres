package com.memgres.engine.parser.ast;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * LOCK [TABLE] [ONLY] name [, ...] [IN lockmode MODE] [NOWAIT]
 */
public final class LockStmt implements Statement {
    public final String tableName;
    public final List<String> tableNames;
    public final String lockMode;
    public final boolean nowait;

    public LockStmt(String tableName, String lockMode, boolean nowait) {
        this.tableName = tableName;
        this.tableNames = Cols.listOf(tableName);
        this.lockMode = lockMode;
        this.nowait = nowait;
    }

    public LockStmt(List<String> tableNames, String lockMode, boolean nowait) {
        this.tableName = tableNames.isEmpty() ? null : tableNames.get(0);
        this.tableNames = tableNames;
        this.lockMode = lockMode;
        this.nowait = nowait;
    }

    public String tableName() { return tableName; }
    public List<String> tableNames() { return tableNames; }
    public String lockMode() { return lockMode; }
    public boolean nowait() { return nowait; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockStmt that = (LockStmt) o;
        return java.util.Objects.equals(tableNames, that.tableNames)
            && java.util.Objects.equals(lockMode, that.lockMode)
            && nowait == that.nowait;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(tableNames, lockMode, nowait);
    }

    @Override
    public String toString() {
        return "LockStmt[tableNames=" + tableNames + ", " + "lockMode=" + lockMode + ", " + "nowait=" + nowait + "]";
    }
}
