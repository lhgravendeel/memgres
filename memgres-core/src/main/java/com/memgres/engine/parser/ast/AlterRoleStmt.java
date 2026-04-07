package com.memgres.engine.parser.ast;

import java.util.Map;
import java.util.Objects;

/**
 * ALTER ROLE name [WITH] options...
 * ALTER ROLE name RENAME TO newname
 */
public final class AlterRoleStmt implements Statement {
    public final String name;
    public final String renameTo;
    public final Map<String, String> options;

    public AlterRoleStmt(String name, String renameTo, Map<String, String> options) {
        this.name = name;
        this.renameTo = renameTo;
        this.options = options;
    }

    public String name() { return name; }
    public String renameTo() { return renameTo; }
    public Map<String, String> options() { return options; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterRoleStmt that = (AlterRoleStmt) o;
        return Objects.equals(name, that.name)
            && Objects.equals(renameTo, that.renameTo)
            && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, renameTo, options);
    }

    @Override
    public String toString() {
        return "AlterRoleStmt[name=" + name + ", renameTo=" + renameTo + ", options=" + options + "]";
    }
}
