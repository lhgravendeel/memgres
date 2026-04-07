package com.memgres.engine.parser.ast;

/**
 * ALTER SEQUENCE name [RESTART [WITH n]] [INCREMENT BY n] [MINVALUE n] [MAXVALUE n] [START WITH n] [CYCLE|NO CYCLE]
 */
public final class AlterSequenceStmt implements Statement {
    public final String name;
    public final boolean restart;
    public final Long restartWith;
    public final Long incrementBy;
    public final Long minValue;
    public final Long maxValue;
    public final Long startWith;
    public final Boolean cycle;
    public final String ownerTo;

    public AlterSequenceStmt(
            String name,
            boolean restart,
            Long restartWith,
            Long incrementBy,
            Long minValue,
            Long maxValue,
            Long startWith,
            Boolean cycle,
            String ownerTo
    ) {
        this.name = name;
        this.restart = restart;
        this.restartWith = restartWith;
        this.incrementBy = incrementBy;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.startWith = startWith;
        this.cycle = cycle;
        this.ownerTo = ownerTo;
    }

    /** Backward-compatible constructor without ownerTo. */
    public AlterSequenceStmt(String name, boolean restart, Long restartWith, Long incrementBy,
                             Long minValue, Long maxValue, Long startWith, Boolean cycle) {
        this(name, restart, restartWith, incrementBy, minValue, maxValue, startWith, cycle, null);
    }
    /** OWNER TO only constructor. */
    public AlterSequenceStmt(String name, String ownerTo) {
        this(name, false, null, null, null, null, null, null, ownerTo);
    }

    public String name() { return name; }
    public boolean restart() { return restart; }
    public Long restartWith() { return restartWith; }
    public Long incrementBy() { return incrementBy; }
    public Long minValue() { return minValue; }
    public Long maxValue() { return maxValue; }
    public Long startWith() { return startWith; }
    public Boolean cycle() { return cycle; }
    public String ownerTo() { return ownerTo; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterSequenceStmt that = (AlterSequenceStmt) o;
        return java.util.Objects.equals(name, that.name)
            && restart == that.restart
            && java.util.Objects.equals(restartWith, that.restartWith)
            && java.util.Objects.equals(incrementBy, that.incrementBy)
            && java.util.Objects.equals(minValue, that.minValue)
            && java.util.Objects.equals(maxValue, that.maxValue)
            && java.util.Objects.equals(startWith, that.startWith)
            && java.util.Objects.equals(cycle, that.cycle)
            && java.util.Objects.equals(ownerTo, that.ownerTo);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, restart, restartWith, incrementBy, minValue, maxValue, startWith, cycle, ownerTo);
    }

    @Override
    public String toString() {
        return "AlterSequenceStmt[name=" + name + ", " + "restart=" + restart + ", " + "restartWith=" + restartWith + ", " + "incrementBy=" + incrementBy + ", " + "minValue=" + minValue + ", " + "maxValue=" + maxValue + ", " + "startWith=" + startWith + ", " + "cycle=" + cycle + ", " + "ownerTo=" + ownerTo + "]";
    }
}
