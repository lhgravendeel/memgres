package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * Represents a stored PL/pgSQL function or procedure.
 */
public class PgFunction {

        public static final class Param {
        public final String name;
        public final String typeName;
        public final String mode;
        public final String defaultExpr;

        public Param(String name, String typeName, String mode, String defaultExpr) {
            this.name = name;
            this.typeName = typeName;
            this.mode = mode;
            this.defaultExpr = defaultExpr;
        }

        public String name() { return name; }
        public String typeName() { return typeName; }
        public String mode() { return mode; }
        public String defaultExpr() { return defaultExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Param that = (Param) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(typeName, that.typeName)
                && java.util.Objects.equals(mode, that.mode)
                && java.util.Objects.equals(defaultExpr, that.defaultExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, typeName, mode, defaultExpr);
        }

        @Override
        public String toString() {
            return "Param[name=" + name + ", " + "typeName=" + typeName + ", " + "mode=" + mode + ", " + "defaultExpr=" + defaultExpr + "]";
        }
    }

    private String name;
    private final String returnType;
    private final String body;
    private final String language;
    private final List<Param> params;
    private final boolean procedure;
    private String schemaName;
    private boolean securityDefiner;
    private boolean strict;
    private String volatility; // "VOLATILE" (default), "STABLE", or "IMMUTABLE"
    private java.util.Map<String, String> setClauses; // function-level GUC overrides
    private String owner;
    private boolean leakproof;
    private double cost = 100;       // default COST for PL/pgSQL; SQL functions default to 1 in PG
    private double rows;             // estimated rows for set-returning functions
    private String parallel;         // "UNSAFE" (default), "RESTRICTED", or "SAFE"
    private boolean windowFunction;  // declared WINDOW, which PG records as prokind='w'

    public PgFunction(String name, String returnType, String body, String language) {
        this(name, returnType, body, language, Cols.listOf(), false);
    }

    public PgFunction(String name, String returnType, String body, String language,
                      List<Param> params, boolean procedure) {
        this.name = name;
        this.returnType = returnType;
        this.body = body;
        this.language = language;
        this.params = params;
        this.procedure = procedure;
    }

    public String getName() { return name; }
    public String getReturnType() { return returnType; }
    public String getBody() { return body; }
    public String getLanguage() { return language; }
    public List<Param> getParams() { return params; }

    /** True when the declared result is {@code record} or {@code SETOF record}. */
    public boolean declaresRecordResult() {
        if (returnType == null) return false;
        String bare = returnType.trim();
        if (bare.length() > 6 && bare.substring(0, 6).equalsIgnoreCase("SETOF ")) {
            bare = bare.substring(6).trim();
        }
        return "record".equalsIgnoreCase(bare);
    }

    /** True when the declaration answers a set of rows rather than a single value. */
    public boolean isSetReturning() {
        if (returnType != null) {
            String bare = returnType.trim();
            if (bare.length() > 6 && bare.substring(0, 6).equalsIgnoreCase("SETOF ")) return true;
            if (bare.equalsIgnoreCase("TABLE")) return true;
        }
        return false;
    }

    /** True when the signature already names the result columns, as OUT or TABLE parameters. */
    public boolean hasOutParams() {
        for (Param p : params) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode) || "INOUT".equals(mode) || "TABLE".equals(mode)) return true;
        }
        return false;
    }

    public boolean isProcedure() { return procedure; }
    public boolean isWindowFunction() { return windowFunction; }
    public void setWindowFunction(boolean windowFunction) { this.windowFunction = windowFunction; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schema) { this.schemaName = schema; }
    public boolean isSecurityDefiner() { return securityDefiner; }
    public void setSecurityDefiner(boolean securityDefiner) { this.securityDefiner = securityDefiner; }
    public boolean isStrict() { return strict; }
    public void setStrict(boolean strict) { this.strict = strict; }
    public String getVolatility() { return volatility; }
    public void setVolatility(String volatility) { this.volatility = volatility; }
    public java.util.Map<String, String> getSetClauses() { return setClauses; }
    public void setSetClauses(java.util.Map<String, String> setClauses) { this.setClauses = setClauses; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
    public void setName(String name) { this.name = name; }
    public boolean isLeakproof() { return leakproof; }
    public void setLeakproof(boolean leakproof) { this.leakproof = leakproof; }
    public double getCost() { return cost; }
    public void setCost(double cost) { this.cost = cost; }
    public double getRows() { return rows; }
    public void setRows(double rows) { this.rows = rows; }
    public String getParallel() { return parallel; }
    public void setParallel(String parallel) { this.parallel = parallel; }

    private boolean atomicBody;
    public boolean isAtomicBody() { return atomicBody; }
    public void setAtomicBody(boolean atomicBody) { this.atomicBody = atomicBody; }

    /**
     * Whether the definition wrote a SQL-standard body -- BEGIN ATOMIC ... END, or RETURN expr.
     * PostgreSQL parses one of those when the routine is defined, which is what leaves a recorded
     * dependency on every relation the body names; a body written as a string leaves none, because
     * nothing reads it until the routine is called.
     */
    private boolean sqlStandardBody;
    public boolean isSqlStandardBody() { return sqlStandardBody; }
    public void setSqlStandardBody(boolean sqlStandardBody) { this.sqlStandardBody = sqlStandardBody; }

    private final java.util.concurrent.atomic.AtomicLong callCount = new java.util.concurrent.atomic.AtomicLong(0);
    public long getCallCount() { return callCount.get(); }
    public void incrementCallCount() { callCount.incrementAndGet(); }
}
