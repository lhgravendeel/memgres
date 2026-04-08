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
}
