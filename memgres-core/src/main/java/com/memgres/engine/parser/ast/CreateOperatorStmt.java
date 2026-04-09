package com.memgres.engine.parser.ast;

/**
 * CREATE OPERATOR name (LEFTARG = type, RIGHTARG = type, FUNCTION|PROCEDURE = func, ...)
 */
public final class CreateOperatorStmt implements Statement {
    public final String schema;        // may be null (default: public)
    public final String name;          // operator name (e.g., +++, ===)
    public final String leftArg;       // left operand type, null for prefix unary
    public final String rightArg;      // right operand type, null for postfix unary
    public final String function;      // implementing function name
    public final String commutator;    // commutator operator name (may be null)
    public final String negator;       // negator operator name (may be null)
    public final String restrict;      // restriction selectivity estimator (may be null)
    public final String join;          // join selectivity estimator (may be null)
    public final boolean hashes;
    public final boolean merges;

    public CreateOperatorStmt(String schema, String name, String leftArg, String rightArg,
                              String function, String commutator, String negator,
                              String restrict, String join, boolean hashes, boolean merges) {
        this.schema = schema;
        this.name = name;
        this.leftArg = leftArg;
        this.rightArg = rightArg;
        this.function = function;
        this.commutator = commutator;
        this.negator = negator;
        this.restrict = restrict;
        this.join = join;
        this.hashes = hashes;
        this.merges = merges;
    }

    public String schema() { return schema; }
    public String name() { return name; }
    public String leftArg() { return leftArg; }
    public String rightArg() { return rightArg; }
    public String function() { return function; }
    public String commutator() { return commutator; }
    public String negator() { return negator; }
    public String restrict() { return restrict; }
    public String join() { return join; }
    public boolean hashes() { return hashes; }
    public boolean merges() { return merges; }
}
