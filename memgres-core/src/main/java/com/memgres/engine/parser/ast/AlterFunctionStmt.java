package com.memgres.engine.parser.ast;

import java.util.Map;
import java.util.Objects;

/**
 * ALTER FUNCTION|PROCEDURE name(args) action
 *
 * Supports: RENAME TO, SET SCHEMA, OWNER TO, and attribute changes
 * (volatility, strict, security, cost, rows, parallel, leakproof, SET/RESET config).
 */
public final class AlterFunctionStmt implements Statement {
    public final String name;
    public final String schema;               // null if unqualified
    public final boolean isProcedure;
    public final boolean ifExists;
    public final Action action;
    public final String targetValue;          // new name, new schema, new owner, or null
    public final String volatility;           // VOLATILE, STABLE, IMMUTABLE or null
    public final Boolean strict;              // true=STRICT, false=CALLED ON NULL INPUT, null=unchanged
    public final Boolean securityDefiner;     // true=DEFINER, false=INVOKER, null=unchanged
    public final Boolean leakproof;           // true/false/null
    public final Double cost;                 // COST value or null
    public final Double rows;                 // ROWS value or null
    public final String parallel;             // SAFE, RESTRICTED, UNSAFE or null
    public final Map<String, String> setClauses; // SET config_param = value (null=unchanged)
    public final java.util.List<String> resetParams; // RESET config_param names (null=unchanged)

    public AlterFunctionStmt(
            String name,
            String schema,
            boolean isProcedure,
            boolean ifExists,
            Action action,
            String targetValue,
            String volatility,
            Boolean strict,
            Boolean securityDefiner,
            Boolean leakproof,
            Double cost,
            Double rows,
            String parallel,
            Map<String, String> setClauses,
            java.util.List<String> resetParams
    ) {
        this.name = name;
        this.schema = schema;
        this.isProcedure = isProcedure;
        this.ifExists = ifExists;
        this.action = action;
        this.targetValue = targetValue;
        this.volatility = volatility;
        this.strict = strict;
        this.securityDefiner = securityDefiner;
        this.leakproof = leakproof;
        this.cost = cost;
        this.rows = rows;
        this.parallel = parallel;
        this.setClauses = setClauses;
        this.resetParams = resetParams;
    }

    public enum Action {
        RENAME_TO,
        SET_SCHEMA,
        OWNER_TO,
        SET_ATTRIBUTES   // volatility, strict, security, cost, rows, parallel, leakproof, SET/RESET
    }

    /** Convenience factory for RENAME TO. */
    public static AlterFunctionStmt renameTo(String name, String schema, boolean isProcedure, boolean ifExists, String newName) {
        return new AlterFunctionStmt(name, schema, isProcedure, ifExists, Action.RENAME_TO, newName,
                null, null, null, null, null, null, null, null, null);
    }

    /** Convenience factory for SET SCHEMA. */
    public static AlterFunctionStmt setSchema(String name, String schema, boolean isProcedure, boolean ifExists, String newSchema) {
        return new AlterFunctionStmt(name, schema, isProcedure, ifExists, Action.SET_SCHEMA, newSchema,
                null, null, null, null, null, null, null, null, null);
    }

    /** Convenience factory for OWNER TO. */
    public static AlterFunctionStmt ownerTo(String name, String schema, boolean isProcedure, boolean ifExists, String newOwner) {
        return new AlterFunctionStmt(name, schema, isProcedure, ifExists, Action.OWNER_TO, newOwner,
                null, null, null, null, null, null, null, null, null);
    }

    public String name() { return name; }
    public String schema() { return schema; }
    public boolean isProcedure() { return isProcedure; }
    public boolean ifExists() { return ifExists; }
    public Action action() { return action; }
    public String targetValue() { return targetValue; }
    public String volatility() { return volatility; }
    public Boolean strict() { return strict; }
    public Boolean securityDefiner() { return securityDefiner; }
    public Boolean leakproof() { return leakproof; }
    public Double cost() { return cost; }
    public Double rows() { return rows; }
    public String parallel() { return parallel; }
    public Map<String, String> setClauses() { return setClauses; }
    public java.util.List<String> resetParams() { return resetParams; }

    /** Returns the PG command tag: "ALTER FUNCTION" or "ALTER ROUTINE". */
    public String commandTag() {
        return isProcedure ? "ALTER PROCEDURE" : "ALTER FUNCTION";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterFunctionStmt that = (AlterFunctionStmt) o;
        return isProcedure == that.isProcedure
            && ifExists == that.ifExists
            && action == that.action
            && Objects.equals(name, that.name)
            && Objects.equals(targetValue, that.targetValue)
            && Objects.equals(volatility, that.volatility)
            && Objects.equals(strict, that.strict)
            && Objects.equals(securityDefiner, that.securityDefiner)
            && Objects.equals(leakproof, that.leakproof)
            && Objects.equals(cost, that.cost)
            && Objects.equals(rows, that.rows)
            && Objects.equals(parallel, that.parallel)
            && Objects.equals(setClauses, that.setClauses)
            && Objects.equals(resetParams, that.resetParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isProcedure, ifExists, action, targetValue, volatility, strict,
                securityDefiner, leakproof, cost, rows, parallel, setClauses, resetParams);
    }

    @Override
    public String toString() {
        return "AlterFunctionStmt[name=" + name + ", isProcedure=" + isProcedure + ", action=" + action
            + ", targetValue=" + targetValue + "]";
    }
}
