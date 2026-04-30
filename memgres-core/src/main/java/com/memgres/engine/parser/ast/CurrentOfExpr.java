package com.memgres.engine.parser.ast;

/**
 * WHERE CURRENT OF cursor_name — used in UPDATE/DELETE to target the cursor's positioned row.
 */
public final class CurrentOfExpr implements Expression {
    private final String cursorName;

    public CurrentOfExpr(String cursorName) {
        this.cursorName = cursorName;
    }

    public String cursorName() { return cursorName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return java.util.Objects.equals(cursorName, ((CurrentOfExpr) o).cursorName);
    }

    @Override
    public int hashCode() { return java.util.Objects.hash(cursorName); }

    @Override
    public String toString() { return "CurrentOfExpr[cursor=" + cursorName + "]"; }
}
