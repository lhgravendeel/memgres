package com.memgres.engine.parser.ast;

/**
 * CREATE [TEMP] SEQUENCE [IF NOT EXISTS] name [START WITH n] [INCREMENT BY n] [MINVALUE n] [MAXVALUE n] [CYCLE | NO CYCLE]
 */
public final class CreateSequenceStmt implements Statement {
    public final String name;
    public final boolean ifNotExists;
    public final Long startWith;
    public final Long incrementBy;
    public final Long minValue;
    public final Long maxValue;
    public final Boolean cycle;
    public final boolean temporary;

    public CreateSequenceStmt(
            String name,
            boolean ifNotExists,
            Long startWith,
            Long incrementBy,
            Long minValue,
            Long maxValue,
            Boolean cycle,
            boolean temporary
    ) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.startWith = startWith;
        this.incrementBy = incrementBy;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.cycle = cycle;
        this.temporary = temporary;
    }

    /** Backward-compatible constructor without cycle and temporary. */
    public CreateSequenceStmt(String name, boolean ifNotExists, Long startWith, Long incrementBy, Long minValue, Long maxValue) {
        this(name, ifNotExists, startWith, incrementBy, minValue, maxValue, null, false);
    }
    /** Backward-compatible constructor without temporary. */
    public CreateSequenceStmt(String name, boolean ifNotExists, Long startWith, Long incrementBy, Long minValue, Long maxValue, Boolean cycle) {
        this(name, ifNotExists, startWith, incrementBy, minValue, maxValue, cycle, false);
    }

    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }
    public Long startWith() { return startWith; }
    public Long incrementBy() { return incrementBy; }
    public Long minValue() { return minValue; }
    public Long maxValue() { return maxValue; }
    public Boolean cycle() { return cycle; }
    public boolean temporary() { return temporary; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSequenceStmt that = (CreateSequenceStmt) o;
        return java.util.Objects.equals(name, that.name)
            && ifNotExists == that.ifNotExists
            && java.util.Objects.equals(startWith, that.startWith)
            && java.util.Objects.equals(incrementBy, that.incrementBy)
            && java.util.Objects.equals(minValue, that.minValue)
            && java.util.Objects.equals(maxValue, that.maxValue)
            && java.util.Objects.equals(cycle, that.cycle)
            && temporary == that.temporary;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, ifNotExists, startWith, incrementBy, minValue, maxValue, cycle, temporary);
    }

    @Override
    public String toString() {
        return "CreateSequenceStmt[name=" + name + ", " + "ifNotExists=" + ifNotExists + ", " + "startWith=" + startWith + ", " + "incrementBy=" + incrementBy + ", " + "minValue=" + minValue + ", " + "maxValue=" + maxValue + ", " + "cycle=" + cycle + ", " + "temporary=" + temporary + "]";
    }
}
