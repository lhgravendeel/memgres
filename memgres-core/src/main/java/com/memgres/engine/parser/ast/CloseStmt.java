package com.memgres.engine.parser.ast;

/**
 * CLOSE cursor_name | CLOSE ALL
 */
public final class CloseStmt implements Statement {
    public final String cursorName;
    public final boolean all;

    public CloseStmt(String cursorName, boolean all) {
        this.cursorName = cursorName;
        this.all = all;
    }

    public String cursorName() { return cursorName; }
    public boolean all() { return all; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CloseStmt that = (CloseStmt) o;
        return java.util.Objects.equals(cursorName, that.cursorName)
            && all == that.all;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(cursorName, all);
    }

    @Override
    public String toString() {
        return "CloseStmt[cursorName=" + cursorName + ", " + "all=" + all + "]";
    }
}
