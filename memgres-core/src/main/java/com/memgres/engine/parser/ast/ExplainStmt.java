package com.memgres.engine.parser.ast;

/**
 * EXPLAIN [ANALYZE] [VERBOSE] [(options)] statement
 *
 * @param deferredOptionError if non-null, an EXPLAIN option validation error message
 *                            that should be raised only after table-existence checks
 *                            (to match PostgreSQL's error-priority behaviour).
 * @param deferredOptionSqlState the SQLSTATE for the deferred error (e.g. "42601" for
 *                               unrecognized option names, "22023" for invalid values).
 */
public final class ExplainStmt implements Statement {
    public final Statement statement;
    public final boolean analyze;
    public final boolean verbose;
    public final String format;
    public final boolean costs;
    public final String deferredOptionError;
    public final String deferredOptionSqlState;
    public final boolean memory;
    public final boolean serialize;
    public final boolean genericPlan;
    public final boolean buffers;
    public final boolean wal;
    public boolean settings;

    public ExplainStmt(
            Statement statement,
            boolean analyze,
            boolean verbose,
            String format,
            boolean costs,
            String deferredOptionError,
            String deferredOptionSqlState
    ) {
        this(statement, analyze, verbose, format, costs, deferredOptionError, deferredOptionSqlState, false, false, false, false, false);
    }

    public ExplainStmt(
            Statement statement,
            boolean analyze,
            boolean verbose,
            String format,
            boolean costs,
            String deferredOptionError,
            String deferredOptionSqlState,
            boolean memory,
            boolean serialize,
            boolean genericPlan
    ) {
        this(statement, analyze, verbose, format, costs, deferredOptionError, deferredOptionSqlState, memory, serialize, genericPlan, false, false);
    }

    public ExplainStmt(
            Statement statement,
            boolean analyze,
            boolean verbose,
            String format,
            boolean costs,
            String deferredOptionError,
            String deferredOptionSqlState,
            boolean memory,
            boolean serialize,
            boolean genericPlan,
            boolean buffers,
            boolean wal
    ) {
        this.statement = statement;
        this.analyze = analyze;
        this.verbose = verbose;
        this.format = format;
        this.costs = costs;
        this.deferredOptionError = deferredOptionError;
        this.deferredOptionSqlState = deferredOptionSqlState;
        this.memory = memory;
        this.serialize = serialize;
        this.genericPlan = genericPlan;
        this.buffers = buffers;
        this.wal = wal;
    }

    /** Canonical constructor without deferred error. */
    public ExplainStmt(Statement statement, boolean analyze, boolean verbose, String format, boolean costs) {
        this(statement, analyze, verbose, format, costs, null, null);
    }

    /** Backward-compatible constructor. */
    public ExplainStmt(Statement statement, boolean analyze, boolean verbose) {
        this(statement, analyze, verbose, "TEXT", true, null, null);
    }

    /** Constructor without costs (defaults to true). */
    public ExplainStmt(Statement statement, boolean analyze, boolean verbose, String format) {
        this(statement, analyze, verbose, format, true, null, null);
    }

    public Statement statement() { return statement; }
    public boolean analyze() { return analyze; }
    public boolean verbose() { return verbose; }
    public String format() { return format; }
    public boolean costs() { return costs; }
    public String deferredOptionError() { return deferredOptionError; }
    public String deferredOptionSqlState() { return deferredOptionSqlState; }
    public boolean memory() { return memory; }
    public boolean serialize() { return serialize; }
    public boolean genericPlan() { return genericPlan; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplainStmt that = (ExplainStmt) o;
        return java.util.Objects.equals(statement, that.statement)
            && analyze == that.analyze
            && verbose == that.verbose
            && java.util.Objects.equals(format, that.format)
            && costs == that.costs
            && java.util.Objects.equals(deferredOptionError, that.deferredOptionError)
            && java.util.Objects.equals(deferredOptionSqlState, that.deferredOptionSqlState);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(statement, analyze, verbose, format, costs, deferredOptionError, deferredOptionSqlState);
    }

    @Override
    public String toString() {
        return "ExplainStmt[statement=" + statement + ", " + "analyze=" + analyze + ", " + "verbose=" + verbose + ", " + "format=" + format + ", " + "costs=" + costs + ", " + "deferredOptionError=" + deferredOptionError + ", " + "deferredOptionSqlState=" + deferredOptionSqlState + "]";
    }
}
