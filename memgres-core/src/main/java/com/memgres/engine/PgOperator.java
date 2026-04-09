package com.memgres.engine;

/**
 * Represents a user-defined operator created via CREATE OPERATOR.
 */
public class PgOperator {
    private final String name;
    private final String leftArg;      // left operand type, null for prefix unary
    private final String rightArg;     // right operand type, null for postfix unary
    private final String function;     // implementing function name
    private String commutator;         // commutator operator name (may be null)
    private String negator;            // negator operator name (may be null)
    private String restrict;           // restriction selectivity estimator (may be null)
    private String join;               // join selectivity estimator (may be null)
    private boolean hashes;            // supports hash join
    private boolean merges;            // supports merge join
    private String schemaName;         // schema (default: public)
    private String owner;              // owner role name

    public PgOperator(String name, String leftArg, String rightArg, String function) {
        this.name = name;
        this.leftArg = leftArg;
        this.rightArg = rightArg;
        this.function = function;
        this.schemaName = "public";
    }

    public String getName() { return name; }
    public String getLeftArg() { return leftArg; }
    public String getRightArg() { return rightArg; }
    public String getFunction() { return function; }
    public String getCommutator() { return commutator; }
    public void setCommutator(String commutator) { this.commutator = commutator; }
    public String getNegator() { return negator; }
    public void setNegator(String negator) { this.negator = negator; }
    public String getRestrict() { return restrict; }
    public void setRestrict(String restrict) { this.restrict = restrict; }
    public String getJoin() { return join; }
    public void setJoin(String join) { this.join = join; }
    public boolean isHashes() { return hashes; }
    public void setHashes(boolean hashes) { this.hashes = hashes; }
    public boolean isMerges() { return merges; }
    public void setMerges(boolean merges) { this.merges = merges; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    /**
     * Returns the operator kind: 'b' for binary, 'l' for left-unary (prefix).
     */
    public String getKind() {
        if (leftArg == null || leftArg.isEmpty()) return "l";
        return "b";
    }

    /**
     * Returns a key that uniquely identifies this operator (schema + name + arg types).
     */
    public String getKey() {
        String l = leftArg != null ? leftArg.toLowerCase() : "NONE";
        String r = rightArg != null ? rightArg.toLowerCase() : "NONE";
        String s = schemaName != null ? schemaName.toLowerCase() : "public";
        return s + "." + name + "(" + l + "," + r + ")";
    }
}
