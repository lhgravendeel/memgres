package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * TRUNCATE [TABLE] table1 [, table2, ...] [RESTART IDENTITY | CONTINUE IDENTITY] [CASCADE|RESTRICT]
 */
public final class TruncateStmt implements Statement {
    public final List<String> tables;
    public final boolean cascade;
    public final boolean restartIdentity;
    /** Per-table ONLY flags (parallel to {@link #tables}); null means no ONLY anywhere. */
    public final List<Boolean> onlyFlags;

    public TruncateStmt(List<String> tables, boolean cascade, boolean restartIdentity) {
        this(tables, cascade, restartIdentity, null);
    }

    public TruncateStmt(List<String> tables, boolean cascade, boolean restartIdentity, List<Boolean> onlyFlags) {
        this.tables = tables;
        this.cascade = cascade;
        this.restartIdentity = restartIdentity;
        this.onlyFlags = onlyFlags;
    }

    /** Backward-compatible accessor for single-table usage. */
    public String table() { return tables.get(0); }

    public List<String> tables() { return tables; }
    public boolean cascade() { return cascade; }
    public boolean restartIdentity() { return restartIdentity; }

    /** Whether the table at the given index was specified with ONLY. */
    public boolean only(int index) {
        return onlyFlags != null && index < onlyFlags.size() && Boolean.TRUE.equals(onlyFlags.get(index));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TruncateStmt that = (TruncateStmt) o;
        return java.util.Objects.equals(tables, that.tables)
            && cascade == that.cascade
            && restartIdentity == that.restartIdentity;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(tables, cascade, restartIdentity);
    }

    @Override
    public String toString() {
        return "TruncateStmt[tables=" + tables + ", " + "cascade=" + cascade + ", " + "restartIdentity=" + restartIdentity + "]";
    }
}
