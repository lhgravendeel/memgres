package com.memgres.engine;

/**
 * Represents a user-defined aggregate function created via CREATE AGGREGATE.
 * PG aggregates work as a state machine: SFUNC(state, value) is called for each row,
 * and optionally FINALFUNC(state) produces the final result.
 */
public class PgAggregate {
    private final String name;
    private final String sfunc;       // state transition function name
    private final String stype;       // state value type
    private final String initcond;    // initial state value (may be null)
    private final String finalfunc;   // final function name (may be null)
    private final String combinefunc; // combine function for parallel aggregation (may be null)
    private final String sortop;      // sort operator (may be null)
    private final String[] argTypes;  // argument types for the aggregate
    private String schemaName;        // schema where the aggregate is defined

    public PgAggregate(String name, String sfunc, String stype, String initcond,
                       String finalfunc, String combinefunc, String sortop, String[] argTypes) {
        this.name = name;
        this.sfunc = sfunc;
        this.stype = stype;
        this.initcond = initcond;
        this.finalfunc = finalfunc;
        this.combinefunc = combinefunc;
        this.sortop = sortop;
        this.argTypes = argTypes;
    }

    public String getName() { return name; }
    public String getSfunc() { return sfunc; }
    public String getStype() { return stype; }
    public String getInitcond() { return initcond; }
    public String getFinalfunc() { return finalfunc; }
    public String getCombinefunc() { return combinefunc; }
    public String getSortop() { return sortop; }
    public String[] getArgTypes() { return argTypes; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    /**
     * The moving-aggregate half of the definition, and how safely the aggregate may be run in
     * parallel.
     *
     * <p>These are what a window frame that slides forward uses: MSFUNC adds a row and MINVFUNC
     * takes one away, over an accumulator of its own type and starting value. Consumed by the
     * parser and thrown away, an aggregate declared with all of them was recorded as having none,
     * so pg_aggregate said the server could not compute it that way -- and PARALLEL SAFE was read
     * and forgotten, so pg_proc called every aggregate unsafe.
     */
    private String mtransfn;
    private String minvtransfn;
    private String mstype;
    private String minitcond;
    private String parallel;

    public String getMtransfn() { return mtransfn; }
    public String getMinvtransfn() { return minvtransfn; }
    public String getMstype() { return mstype; }
    public String getMinitcond() { return minitcond; }
    public String getParallel() { return parallel; }

    public void setMovingAggregate(String mtransfn, String minvtransfn, String mstype,
                                   String minitcond) {
        this.mtransfn = mtransfn;
        this.minvtransfn = minvtransfn;
        this.mstype = mstype;
        this.minitcond = minitcond;
    }

    public void setParallel(String parallel) { this.parallel = parallel; }

    /**
     * How many of the arguments stand in front of WITHIN GROUP, or -1 for an ordinary aggregate.
     *
     * <p>An ordered-set aggregate is written {@code (direct args ORDER BY sort args)} and is a
     * different kind of aggregate: PostgreSQL records it as kind "o" and says how many of its
     * arguments are direct. Read and thrown away, every such aggregate was recorded as an
     * ordinary one taking no direct arguments at all.
     */
    private int directArgCount = -1;

    public int getDirectArgCount() { return directArgCount; }

    public void setDirectArgCount(int directArgCount) { this.directArgCount = directArgCount; }
}
