package com.memgres.engine.parser.ast;

/**
 * FETCH [direction] [FROM|IN] cursor_name
 * MOVE [direction] [FROM|IN] cursor_name
 */
public final class FetchStmt implements Statement {
    public final Direction direction;
    public final int count;
    public final String cursorName;
    public final boolean isMove;

    public FetchStmt(Direction direction, int count, String cursorName, boolean isMove) {
        this.direction = direction;
        this.count = count;
        this.cursorName = cursorName;
        this.isMove = isMove;
    }

    public enum Direction {
        NEXT, PRIOR, FIRST, LAST, ABSOLUTE, RELATIVE, FORWARD, BACKWARD, FORWARD_ALL, BACKWARD_ALL, ALL
    }

    public Direction direction() { return direction; }
    public int count() { return count; }
    public String cursorName() { return cursorName; }
    public boolean isMove() { return isMove; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchStmt that = (FetchStmt) o;
        return java.util.Objects.equals(direction, that.direction)
            && count == that.count
            && java.util.Objects.equals(cursorName, that.cursorName)
            && isMove == that.isMove;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(direction, count, cursorName, isMove);
    }

    @Override
    public String toString() {
        return "FetchStmt[direction=" + direction + ", " + "count=" + count + ", " + "cursorName=" + cursorName + ", " + "isMove=" + isMove + "]";
    }
}
