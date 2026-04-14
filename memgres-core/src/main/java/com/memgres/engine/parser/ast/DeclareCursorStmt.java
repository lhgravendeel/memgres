package com.memgres.engine.parser.ast;

/**
 * DECLARE name [BINARY] [INSENSITIVE] [NO] SCROLL CURSOR [WITH|WITHOUT HOLD] FOR query
 */
public final class DeclareCursorStmt implements Statement {
    public final String name;
    public final Statement query;
    public final boolean scroll;
    public final boolean withHold;
    public final boolean binary;
    public final boolean explicitNoScroll;

    public DeclareCursorStmt(String name, Statement query, boolean scroll, boolean withHold, boolean binary, boolean explicitNoScroll) {
        this.name = name;
        this.query = query;
        this.scroll = scroll;
        this.withHold = withHold;
        this.binary = binary;
        this.explicitNoScroll = explicitNoScroll;
    }

    public DeclareCursorStmt(String name, Statement query, boolean scroll, boolean withHold, boolean binary) {
        this(name, query, scroll, withHold, binary, false);
    }

    /** Backward-compatible constructor (binary defaults to false). */
    public DeclareCursorStmt(String name, Statement query, boolean scroll, boolean withHold) {
        this(name, query, scroll, withHold, false, false);
    }

    public String name() { return name; }
    public Statement query() { return query; }
    public boolean scroll() { return scroll; }
    public boolean withHold() { return withHold; }
    public boolean binary() { return binary; }
    public boolean explicitNoScroll() { return explicitNoScroll; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeclareCursorStmt that = (DeclareCursorStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(query, that.query)
            && scroll == that.scroll
            && withHold == that.withHold
            && binary == that.binary;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, query, scroll, withHold, binary);
    }

    @Override
    public String toString() {
        return "DeclareCursorStmt[name=" + name + ", " + "query=" + query + ", " + "scroll=" + scroll + ", " + "withHold=" + withHold + ", " + "binary=" + binary + "]";
    }
}
