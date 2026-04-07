package com.memgres.engine.parser.ast;

public final class LockClause {
    public final String mode;
    public final boolean nowait;
    public final boolean skipLocked;

    public LockClause(String mode, boolean nowait, boolean skipLocked) {
        this.mode = mode;
        this.nowait = nowait;
        this.skipLocked = skipLocked;
    }

    public String mode() { return mode; }
    public boolean nowait() { return nowait; }
    public boolean skipLocked() { return skipLocked; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockClause that = (LockClause) o;
        return java.util.Objects.equals(mode, that.mode)
            && nowait == that.nowait
            && skipLocked == that.skipLocked;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(mode, nowait, skipLocked);
    }

    @Override
    public String toString() {
        return "LockClause[mode=" + mode + ", " + "nowait=" + nowait + ", " + "skipLocked=" + skipLocked + "]";
    }
}
