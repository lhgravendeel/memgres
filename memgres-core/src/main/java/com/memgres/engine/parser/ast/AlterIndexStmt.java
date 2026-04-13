package com.memgres.engine.parser.ast;

import java.util.Map;
import java.util.Objects;

/**
 * ALTER INDEX [IF EXISTS] name action
 *
 * Supports: RENAME TO (functional), SET TABLESPACE (no-op),
 * ATTACH PARTITION (no-op), ALTER COLUMN SET STATISTICS (no-op),
 * SET/RESET storage parameters.
 */
public final class AlterIndexStmt implements Statement {
    public final String name;
    public final boolean ifExists;
    public final Action action;
    public final String targetValue;  // new name, tablespace name, or null
    public final Map<String, String> params;  // storage parameters for SET/RESET

    public AlterIndexStmt(String name, boolean ifExists, Action action, String targetValue) {
        this(name, ifExists, action, targetValue, null);
    }

    public AlterIndexStmt(String name, boolean ifExists, Action action, String targetValue, Map<String, String> params) {
        this.name = name;
        this.ifExists = ifExists;
        this.action = action;
        this.targetValue = targetValue;
        this.params = params;
    }

    public enum Action {
        RENAME_TO,
        SET_TABLESPACE,     // no-op for in-memory DB
        ATTACH_PARTITION,   // no-op
        SET_STATISTICS,     // no-op
        SET_PARAMS,         // no-op (SET storage params)
        RESET_PARAMS,       // no-op (RESET storage params)
        NO_OP               // catch-all for unrecognized subclauses
    }

    public String name() { return name; }
    public boolean ifExists() { return ifExists; }
    public Action action() { return action; }
    public String targetValue() { return targetValue; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterIndexStmt that = (AlterIndexStmt) o;
        return ifExists == that.ifExists
            && action == that.action
            && Objects.equals(name, that.name)
            && Objects.equals(targetValue, that.targetValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ifExists, action, targetValue);
    }

    @Override
    public String toString() {
        return "AlterIndexStmt[name=" + name + ", ifExists=" + ifExists
            + ", action=" + action + ", targetValue=" + targetValue + "]";
    }
}
